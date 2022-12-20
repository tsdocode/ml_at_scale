import streamlit as st
from kafka import KafkaHandler
from firebase import RealtimeDB
import pandas as pd
import json
import plotly.express as px
from random import choice
from time import sleep
from streamlit_autorefresh import st_autorefresh
from subprocess import Popen, PIPE
from multiprocessing import Process

handler = KafkaHandler()

st_autorefresh(interval=4000, key="dataframerefresh")


@st.cache(allow_output_mutation=True)
def init_firebase():
    db = RealtimeDB()

    return db

@st.cache(allow_output_mutation=True)
def load_data():
    data = pd.read_csv('data.csv')
    data = data.to_dict(orient="records")
    sleep(1)

    return data


def get_data(db):
    data = db.get("/network_data")
    if data is not None:
        data = [data[i] for i in data]
        headers = data[0].keys()
        for i, _ in enumerate(data):
            data[i] = [str(data[i][j]) for j in data[i]]

        data = pd.DataFrame.from_records(data)
        data.columns = headers
        return data
    else:
        return ""

def produce_data(n=30):
    p = Popen(["python", "producer.py"], stdout=PIPE, stderr=PIPE)
    p.wait()





def main():
    st.sidebar.title("Network Intrusion Detection Simulator")
    st.sidebar.text("Detection network intrusion\nin realtime using Spark & Kafka")
    db = init_firebase()
    data = get_data(db)
    # create three columns
    m1, m2, m3 = st.columns(3)

    # fill in those three columns with respective metrics or KPIs
    m1.metric(
        label="Number of access",
        value=len(data.index),
        delta=len(data.index),
    )

    m2.metric(
        label="Number of intrusion detected",
        value=len(data[data['prediction'] == '1.0']),
        delta=len(data[data['prediction'] == '1.0']) -5,
    )

    m3.metric(
        label="Avg access duration",
        value=int(data['duration'].astype(float).mean())
    )


    fig = px.pie(data, names='prediction', title='Attract percent')

    st.plotly_chart(fig)


    st.dataframe(data)

    slider = st.sidebar.slider("Number of access", 1, 100, 10)

    col1, col2, col3 , col4, col5 = st.sidebar.columns([1,1,4,1,1])
    with col1:
        pass
    with col5:
        button = st.sidebar.button("Start emulator")

    if button:
        p = Process(target=produce_data, args=(slider,))
        p.start()

    


if __name__ == "__main__":
    main()