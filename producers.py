import asyncio
import websockets
import json
import pandas as pd
import joblib
from kafka import KafkaProducer
from datetime import datetime

gbm_model = joblib.load("gbm_model_sampling.pkl")

producer = KafkaProducer(bootstrap_servers='localhost:9092')

async def listen():
    url = "wss://stream.binance.com:9443/ws/bnbusdt@kline_3m"
    
    async with websockets.connect(url) as ws:
        while True:
            msg = await ws.recv()
            json_msg = json.loads(msg)
            
            if 'k' in json_msg:
                kline_data = json_msg['k']
                open_price = kline_data['o']
                high = kline_data['h']
                low = kline_data['l']
                close_price = kline_data['c']
                volume = kline_data['v']
                
                # Tạo DataFrame từ dữ liệu streaming
                new_row = {
                    "Time": datetime.now().isoformat(),
                    "Open": open_price,
                    "High": high,
                    "Low": low,
                    "Close": close_price,
                    "Volume": volume
                }
                streaming_data = pd.DataFrame([new_row])
                
                streaming_data1 = streaming_data.drop(columns=["Time"])
                
                predictions = gbm_model.predict(streaming_data1)
                
                streaming_data["Label"] = predictions
                
                json_data = streaming_data.to_json(orient='records')
                
                producer.send('stock', json_data.encode('utf-8'))
                
                # In kết quả
                print("=============")
                print(streaming_data)
            
            await asyncio.sleep(1)

asyncio.get_event_loop().run_until_complete(listen())
