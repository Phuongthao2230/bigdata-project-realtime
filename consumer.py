import pandas as pd
from kafka import KafkaConsumer
import json
# Khởi tạo Kafka consumer
consumer = KafkaConsumer('stock', bootstrap_servers='localhost:9092')

# Vòng lặp để lắng nghe tin nhắn từ Kafka topic
for msg in consumer:
    # Lấy dữ liệu từ tin nhắn
    json_data = msg.value.decode('utf-8')
    
    # Chuyển đổi JSON thành DataFrame
    streaming_data = pd.read_json(json_data)
    
    # Xử lý dữ liệu như mong muốn
    print("=============")
    print(streaming_data)