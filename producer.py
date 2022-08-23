from ensurepip import bootstrap
import json
from re import S
import time

from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_detail"
ORDER_LIMIT = 15

producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Going to be generating order after 10 seconds")
print("Will generate one unique order every 5 seconds")

for i in range(ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "total_cost": i,
        "items": "burger, sandwich",
    }

    producer.send("order_details", json.dumps(data))
    print(f"Done sending..{i}")
    time.sleep(5)
