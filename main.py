import json
import time

import streamlit as st
import pandas as pd


def load_current_station_info(json_path: str=""):
    with open(json_path, 'r') as file:
        data = json.load(file)

    return data

def update_map(station_dataframe: pd.DataFrame=None):
    return st.map(
        data=station_dataframe,
        latitude='lat',
        longitude='lon',
        size=5,
        use_container_width=True,
        zoom=11,
        color='color'
    )

if __name__ == '__main__':

    st.title("Live Divvy Station Capacity")
    st.write("The maps which appear below are refreshing in 30 second increments to show changes in station usage.")
    st.markdown(":red[Red dot] : Station has nearly reached its capacity for bicycles, docking a bike will be difficult, getting a bike will be easy.")
    st.markdown(":green[Green dot] : Station has a fairly even distribution of bikes available versus open docks. Getting a bike will be easy, docking a bike will be easy.")
    st.markdown(":blue[Blue dot] : Station has a low number of bikes available. Getting a bike will be difficult, docking a bike will be easy.")
    running = True
    while running:
        # get data
        current_station_info = load_current_station_info('./data/station_status_updated.json')
        df = pd.DataFrame(current_station_info)
        with st.empty() as placeholder:
            st.write("Loading data...")
            update_map(station_dataframe=df)
        time.sleep(30)