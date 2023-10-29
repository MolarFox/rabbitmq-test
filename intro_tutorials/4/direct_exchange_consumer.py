#!/usr/bin/env python
import pika
import time
import random
import sys

LOG_SEVERITIES = ["info", "warning", "error"]

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

my_id = "".join(chr(random.randint(65, 90)) for _ in range(2))
severity_bind = sys.argv[2] if len(sys.argv) > 2 else random.choice(LOG_SEVERITIES)

print(f" [CONS-{my_id}] Monitoring severity '{severity_bind}'")

channel.exchange_declare(
    exchange="logs",
    exchange_type="fanout"
)

# NB: If there are no connected consumers, messages are deleted
res = channel.queue_declare(queue="", exclusive=True)
queue_name = res.method.queue

channel.queue_bind(
    exchange="direct_logs",
    queue=queue_name,
    routing_key=severity_bind
)

# To subscribe to all routing keys with the exclusive queue
# for sev in LOG_SEVERITIES:
#     channel.queue_bind(
#         exchange="direct_logs",
#         queue=queue_name,
#         routing_key=sev
#     )

def callback(ch, method, properties, body):
    print(f" [CONS-{my_id}] Received at loglevel {method.routing_key}: {body.decode()}")
    time.sleep(
        len(body) - len(body.decode().rstrip('.'))
    )
    print(f" [CONS-{my_id}] Completed!")
    ch.basic_ack(delivery_tag = method.delivery_tag)

print(f' [CONS-{my_id}] Waiting for messages. To exit press CTRL+C')

channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback
)
channel.start_consuming()