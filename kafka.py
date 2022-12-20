import sys
import os
import json
from dotenv import load_dotenv
import asyncio




load_dotenv()



from confluent_kafka import Producer, Consumer


# Kafka credential
KAFKA_BROKERS="dory.srvs.cloudkafka.com:9094"
KAFKA_USERNAME="ixlwq92s"
KAFKA_PASSWORD="pn7Vq0psXudW1-Rr3JsAwMQMbgAWi0oO"

class KafkaHandler():
    def __init__(self):
        self.__producer_config  = {
            'bootstrap.servers': KAFKA_BROKERS,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': KAFKA_USERNAME,
            'sasl.password': KAFKA_PASSWORD
        }

        self.__consumer_config = {
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': "%s-consumer" % KAFKA_USERNAME,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': KAFKA_USERNAME,
            'sasl.password': KAFKA_PASSWORD
        }

        self.producer = Producer(self.__producer_config)
        self.consumer = Consumer(self.__consumer_config)

    def produce(self, topic, message):
        self.producer.produce(topic, message)
        self.producer.flush()

    def consume(self, topic, on_message_handler, *args, **kwargs):
        print(topic)
        # client.run(TOKEN)
        self.consumer.subscribe([topic])
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}")
            
            
            print('Received message: {}'.format(msg.value().decode('utf-8')))

            data = json.loads(msg.value().decode('utf-8'))

            on_message_handler(data)