#!/usr/bin/env python
import pika
import random

# Simulates loading tasks into a work queue, dots in str message represent simulated task complexity

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
# Work distribution is round robin by default
# Use this config to enable `basic.qos` protocol to distribute to next available worker instead
channel.basic_qos(prefetch_count=1)

channel.queue_declare(queue='task_queue', durable=True)

message = 'Task: ' + '.'*random.randint(1,5) or 'O(1) task'

channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode = pika.spec.PERSISTENT_DELIVERY_MODE
    )
)
print(f" [PUBL] Sent task to broker: {message}")

connection.close()
