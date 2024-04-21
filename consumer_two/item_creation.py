import pika
import json
from pymongo import MongoClient

# MongoDB connection parameters
MONGODB_HOST = "host.docker.internal"
MONGODB_PORT = 27017
MONGODB_DATABASE = "inventory_db"
MONGODB_COLLECTION = "inventory"

# RabbitMQ connection parameters
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_PORT = 5672
RABBITMQ_ITEM_QUEUE = "item_queue"


def insert_into_mongodb(data):
    try:
        # Connect to MongoDB
        client = MongoClient(f"mongodb://{MONGODB_HOST}:{MONGODB_PORT}/")
        db = client[MONGODB_DATABASE]

        # Insert data into MongoDB collection
        collection = db[MONGODB_COLLECTION]
        collection.insert_one(data)
        print("Data inserted into MongoDB successfully")
    except Exception as e:
        print("Failed to insert data into MongoDB:", str(e))


def consume_messages():
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        channel = connection.channel()

        # Declare the queue
        channel.queue_declare(queue=RABBITMQ_ITEM_QUEUE)

        # Define callback function to handle incoming messages
        def callback(ch, method, properties, body):
            message = json.loads(body.decode("utf-8"))
            print("Received message:", message)

            # Check if the message is for item creation
            if "action" in message and message["action"] == "create_item":
                # Insert the data into MongoDB
                insert_into_mongodb(message["data"])

        # Consume messages from the queue
        channel.basic_consume(
            queue=RABBITMQ_ITEM_QUEUE, on_message_callback=callback, auto_ack=True
        )

        print("Waiting for item creation messages...")
        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        print(f"RabbitMQ connection error: {e}")
    except Exception as e:
        print("An error occurred:", str(e))


if __name__ == "__main__":
    consume_messages()
