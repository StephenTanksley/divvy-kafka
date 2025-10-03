## Project Overview

This project builds a real-time data pipeline that ingests live Divvy BikeShare station status from Lyft’s API, streams it through Apache Kafka, and visualizes availability on a Streamlit map. It solves common challenges in scalable, decoupled event-driven applications—automating data ingestion, buffering, and live presentation without complex orchestration.

![overview](divvy-kafka.gif)

#### Problems Solved

- Continuous, up-to-date view of bike station availability
- Decoupled ingestion, processing, and visualization layers
- Easy scaling and fault tolerance via Kafka

#### Technology Stack

- Python Kafka client (producer & consumer)
- Apache Kafka for message brokering
- Lyft/Divvy REST API for live station data (JSON payload)
- Streamlit for interactive web visualization

#### Data Flow

Producer → Kafka → Consumer → JSON → Streamlit Map

- Producer
  - Polls Lyft/Divvy API at configurable intervals
  - Serializes station status into JSON
  - Publishes to a Kafka topic
- Kafka 
  - Buffers and replicates messages for fault tolerance
  - Supports horizontal scaling of consumers
- Consumer
  - Subscribes to topic
  - Enriches JSON payload
  - Saves enriched JSON payload to intermediate store (`/data/station_status_updated.json`)
- Streamlit Map
  - Reads from intermediate store (`/data/station_status_updated.json`)
  - Renders live station availability on an interactive map

#### Quickstart Diagram

```text
┌──────────┐   ┌────────┐   ┌───────────┐   ┌───────┐   ┌───────────────┐
│ Producer │──▶│ Kafka  │──▶│ Consumer  │──▶│ JSON  │──▶│ Streamlit Map │
└──────────┘   └────────┘   └───────────┘   └───────┘   └───────────────┘
```

## Setup & Quick Start

This guide walks you through cloning the repo, launching Kafka in Docker, installing Python dependencies, and running the sample Divvy bikeshare producer and consumer.

#### Prerequisites

- Docker & docker compose (v1.27+)
- Python 3.9+
- Git

1. Clone the Repository

```bash
git clone https://github.com/StephenTanksley/divvy-kafka.git
cd divvy-kafka
```

2. Launch Kafka via Docker

The provided docker compose.yml brings up:

- A Kafka broker on localhost:9092
- An init container that creates topic station-status

Start the stack:

```bash
docker compose up -d
```

Verify the broker and topic:

```bash
# Broker health
docker compose ps broker

# Topic list
docker compose exec broker \
 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
# → station-status
```

3. Install Python Dependencies

Create and activate a virtual environment, then install:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements-kafka.txt
```

Confirm key packages:

```bash
pip freeze | grep -E "kafka-python|pandas|streamlit"
# kafka-python==2.2.15
# pandas==1.5.3
# streamlit==1.12.0
```

4. Run the Divvy Producer

The producer streams live Divvy station-status JSON into Kafka.

Run the producer:

```bash
python divvy_producer.py
```

5. Run the Divvy Consumer

The consumer pulls data from the Divvy `station-status` topic from Kafka and enriches it with static data from the `station_info.json` file located in the `/data` directory.

Launch the consumer:

```bash
python consumer.py
```


6. Run Tests

Open the map by running Streamlit
```bash
cd divvy-kafka
streamlit run main.py
```

7. Run Tests

Tests are run using PyTest. To run the whole suite run the following:

```bash
cd divvy-kafka
pytest
```

To run just a single file, run the following:
```bash
cd divvy-kafka
pytest ./tests/test_divvy_producer.py
```

To run a single test within the file, specify the test name you'd like to run:

```bash
cd divvy-kafka
pytest ./tests/test_divvy_producer.py::test_producer_send_failure
```

### Next Steps

- Explore integrating Flink 
- Write integration tests to ensure that both producer and consumer are working together nicely.
- Debug errors with Streamlit's map propagation/update function (which I caused)
- Integrate producer and consumer created here with the larger scale Divvy bikeshare project.