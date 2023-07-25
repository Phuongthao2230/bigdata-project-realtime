import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import random
from itertools import count
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
plt.style.use('fivethirtyeight')
import time
import plotly.express as px # interactive charts

consumer = KafkaConsumer('stock', bootstrap_servers='localhost:9092')

def read_streaming_data():
    for msg in consumer:
        # Lấy dữ liệu từ tin nhắn
        json_data = msg.value.decode('utf-8')
        # Chuyển đổi JSON thành DataFrame
        streaming_data = pd.read_json(json_data)
        return streaming_data
    
def read_streaming_data1():
    for msg in consumer:
        # Lấy dữ liệu từ tin nhắn
        json_data = msg.value.decode('utf-8')
        # Chuyển đổi JSON thành DataFrame
        streaming_data = pd.read_json(json_data)
        streaming_data = streaming_data[['Time', 'Label']]
        return streaming_data

def main():
    placeholder = st.empty()
    st.title("Demo Kafka Streaming with Streamlit")
    st.title("Dataframe")
    data_placeholder = st.empty()
    st.title("Line Chart")
    placeholder = st.empty()
    streaming_data = pd.DataFrame()
    while True:
 
        data = read_streaming_data()

        data_placeholder.write(data)

        new_data = read_streaming_data1()
    
        streaming_data = pd.concat([streaming_data, new_data], ignore_index=True)
        
        with placeholder.container():
            st.markdown("### Line Chart")
            fig = px.line(data_frame=streaming_data, y = 'Label', x = 'Time')
            st.write(fig)
            time.sleep(1)

if __name__ == '__main__':
    main()



