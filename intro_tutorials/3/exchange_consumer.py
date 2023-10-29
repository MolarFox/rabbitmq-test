#!/usr/bin/env python
import pika
import time
import random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

my_id = "".join(chr(random.randint(65, 90)) for _ in range(2))

channel.exchange_declare(
    exchange="logs",
    exchange_type="fanout"
)

# NB: If there are no connected consumers, messages are deleted
res = channel.queue_declare(queue="", exclusive=True)
queue_name = res.method.queue
channel.queue_bind(
    exchange="logs",
    queue=queue_name
)

def callback(ch, method, properties, body):
    print(f" [CONS-{my_id}] Received {body.decode()}")
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