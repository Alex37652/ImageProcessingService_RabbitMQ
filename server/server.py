import logging
import pika
import os

from flask import Flask, request
from typing import List, Optional

from config import IMAGES_ENDPOINT, DATA_DIR
import threading


class Server:
    # TODO: Your code here.
    def __init__(self, host, port):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='add_image', durable=True)
        self.channel.confirm_delivery()
        self.id = 0
        self.get_image_ids: list = []
        self._lock = threading.Lock()
        self._lock_list_ids = threading.Lock()
        self.channel.queue_declare(queue='callback', durable=True)
        self.channel.basic_qos(prefetch_count=1)  # This uses the basic.qos protocol method to tell RabbitMQ not to give more than one message to a worker at a time
        self.channel.basic_consume(queue='callback', on_message_callback=self.put_id_in_list, auto_ack=True)

        


    def store_image(self, image: str) -> int:
        thread = threading.Thread(target = self.push_image, args=(image, self.id))
        thread.start()
        self.id += 1
        return self.id - 1

    def get_processed_images(self) -> List[int]:
        with self._lock_list_ids:
            return self.get_image_ids
       

    def get_image_description(self, image_id: str) -> Optional[str]:
        if os.path.exists("/data/" + image_id + ".txt"):
            file = open("/data/" + image_id + ".txt", "r")
            return file.read()
        return None
    
    def push_image(self, image, id):
        with self._lock:
            self.channel.basic_publish(
                    exchange='',
                    routing_key='add_image',
                    body=str(id) + ' ' + image,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent. Marking messages as persistent doesn't fully guarantee that a message won't be lost. Although it tells RabbitMQ to save the message to disk, there is still a short time window when RabbitMQ has accepted a message and hasn't saved it yet. Also, RabbitMQ doesn't do fsync(2) for every message
                        reply_to="callback",
                ))
            self.connection.process_data_events(time_limit=None)
    
    def put_id_in_list(self, ch, method, properties, body):
            with self._lock_list_ids:
                id = int(body.decode())
                self.get_image_ids.append(id)
    

        


def create_app() -> Flask:
    """
    Create flask application
    """
    app = Flask(__name__)

    server = Server('rabbitmq', 5672)

    @app.route(IMAGES_ENDPOINT, methods=['POST'])
    def add_image():
        body = request.get_json(force=True)
        image_id = server.store_image(body['image_url'])
        return {"image_id": image_id}

    @app.route(IMAGES_ENDPOINT, methods=['GET'])
    def get_image_ids():
        image_ids = server.get_processed_images()
        return {"image_ids": image_ids}

    @app.route(f'{IMAGES_ENDPOINT}/<string:image_id>', methods=['GET'])
    def get_processing_result(image_id):
        result = server.get_image_description(image_id)
        if result is None:
            return "Image not found.", 404
        else:
            return {'description': result}

    return app


app = create_app()

if __name__ == '__main__':
    logging.basicConfig()
    app.run(host='0.0.0.0', port=5000)
