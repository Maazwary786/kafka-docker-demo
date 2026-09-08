from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'test-topic'

for i in range(10):
    message = {'number': i, 'text': f'Hello Kafka This is Mohd Mouaz Ahmed{i}'}
    producer.send(topic, value=message)
    print(f"Sent: {message}")
    time.sleep(1)

producer.flush()
print("Done sending messages.")