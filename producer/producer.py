import time
import json
import random
from datetime import datetime
from data_generator import get_simulated_data_event
from kafka import KafkaProducer

def serializer(message):
    return json.dumps(message).encode('utf-8')

def start_producer():
    print("Trying to start producer!")
    tries = 0
    connectionFound = False
    while tries < 6 and not connectionFound:
        try:
            producerConnection = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=serializer
            )
            connectionFound = True
        except:
            print("Kafka broker not found! Waiting before trying again...")
            time.sleep(5)
            tries += 1
    if tries == 6:
        print("Something is wrong.")
        raise Exception

    return producerConnection


producer = start_producer()

if __name__ == '__main__':
    while True:
        event_message = get_simulated_data_event()
        print(f'Producing message @ {datetime.now()} | Message = {str(event_message)}')
        producer.send('messages', event_message)

        time_to_sleep = random.randint(1, 3)
        time.sleep(time_to_sleep)