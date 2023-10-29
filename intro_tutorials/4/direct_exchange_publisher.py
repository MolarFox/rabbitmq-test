#!/usr/bin/env python
import pika
import random

# Simulates loading tasks into a work queue, dots in str message represent simulated task complexity

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.basic_qos(prefetch_count=1)

channel.exchange_declare(
    exchange="direct_logs",
    exchange_type="direct"
)

message = 'Task: ' + '.'*random.randint(1,5)
severity = random.choice(["info", "warning", "error"])

channel.basic_publish(
    exchange='direct_logs',
    routing_key=severity,
    body=message,
    properties=pika.BasicProperties(
        delivery_mode = pika.spec.PERSISTENT_DELIVERY_MODE
    )
)
print(f" [PUBL] Sent task to broker: {message}")

connection.close()
