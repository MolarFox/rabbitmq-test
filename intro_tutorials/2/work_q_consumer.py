#!/usr/bin/env python
import pika
import time
import random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)

my_id = "".join(chr(random.randint(65, 90)) for _ in range(2))

def callback(ch, method, properties, body):
    print(f" [CONS-{my_id}] Received {body.decode()}")
    time.sleep(
        len(body) - len(body.decode().rstrip('.'))
    )
    print(f" [CONS-{my_id}] Completed!")
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_consume(
    queue='task_queue',
    # auto_ack=True,
    on_message_callback=callback
)

print(f' [CONS-{my_id}] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()