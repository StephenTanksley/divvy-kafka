from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_station_info_source_kafka(t_env):
    ddl = """
        CREATE TABLE station_info (
            station_id STRING,
            name STRING,
            lat FLOAT,
            lon FLOAT,
            address STRING,
            capacity INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/flink/data/station_info.json',
            'format' = 'json'
        )
    """
    t_env.execute_sql(ddl)
    return "station_info"


def create_station_status_source_kafka(t_env):
    ddl = """
        CREATE TABLE station_status (
            station_id STRING,
            num_docks_available NUMERIC,
            num_docks_disabled NUMERIC,
            num_bikes_available NUMERIC,
            num_ebikes_available NUMERIC,
            num_scooters_available NUMERIC,
            station_update_time TIMESTAMP(3),
            WATERMARK FOR station_update_time AS station_update_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'station-status',
            'properties.bootstrap.servers' = 'kafka_broker:29092',
            'scan.startup.mode' = 'latest-offset',
            'value.format' = 'json'
        )
    """

    # station_update_time TIMESTAMP(3),
    #             WATERMARK FOR station_update_time AS station_update_time - INTERVAL '5' SECOND
    t_env.execute_sql(ddl)
    return "station_status"


# --- Enriched Output Sink ---
def create_station_status_enriched_kafka_sink(t_env):
    ddl = """
        CREATE TABLE station_status_enriched (
            station_id STRING,
            name STRING,
            capacity NUMERIC,
            num_docks_available NUMERIC,
            num_docks_disabled NUMERIC,
            num_bikes_available NUMERIC,
            num_ebikes_available NUMERIC,
            num_scooters_available NUMERIC,
            lat FLOAT,
            lon FLOAT,
            address STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'station-status-enriched',
            'properties.bootstrap.servers' = 'kafka_broker:29092',
            'value.format' = 'json'
        )
    """
    t_env.execute_sql(ddl)
    return "station_status_enriched"


# --- Main Enrichment Job ---
def enrich_live_data():
    # Set up the environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # every 10 seconds
    env.set_parallelism(1)

    # Set up table environment in streaming mode
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create source & sink tables
        station_info_table = create_station_info_source_kafka(table_env)
        station_status_table = create_station_status_source_kafka(table_env)
        station_enriched_sink = create_station_status_enriched_kafka_sink(table_env)

        # Temporal join: enrich live status with station info
        table_env.execute_sql(f"""
            INSERT INTO {station_enriched_sink}
            SELECT
                sst.station_id,
                info.name AS name,
                info.capacity AS capacity,
                sst.num_docks_available AS num_docks_available,
                sst.num_docks_disabled AS num_docks_disabled,
                sst.num_bikes_available AS num_bikes_available,
                sst.num_ebikes_available AS num_ebikes_available,
                sst.num_scooters_available AS num_scooters_available,
                info.lat AS latitude,
                info.lon AS longitude,
                info.address AS address
            FROM {station_status_table} AS sst
            JOIN {station_info_table} info
                ON sst.station_id = info.station_id
        """).wait()
    except Exception as e:
        print("Writing records from Kafka to Kafka failed: ", str(e))


if __name__ == '__main__':
    enrich_live_data()

# Re: PROCTIME() - https://stackoverflow.com/questions/79132068/whether-processing-time-temporal-join-supports-for-system-time-as-of-syntax-or-n
# Re: FOR SYSTEM TIME AS OF syntax -  https://stackoverflow.com/questions/71209332/what-is-the-difference-between-lookup-and-processing-time-temporal-join-in-flink
# docker compose down \
#   && docker compose build \
#   && docker compose up -d \
#   &&  docker exec -it flink_jobmanager ./bin/flink run -py /opt/flink/data/flink_transformer.py