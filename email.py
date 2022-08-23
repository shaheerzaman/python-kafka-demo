import json
from re import S
from kafak import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC, bootstrap_servers="localhost:29092"
)

emails_sent_so_far = set()
print("Gonna start listening")

while True:
    for message in consumer:
        consumed_message = json.loads(message.value)
        customer_email = consumed_message["customer_email"]
        print(f"sending email to {customer_email}")
        emails_sent_so_far.add(customer_email)
        print(f"so far emails sent to {len(emails_sent_so_far)} unique emails")
