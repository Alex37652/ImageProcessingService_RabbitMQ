from caption import get_image_caption
import pika
import caption
import os


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', port=5672))
    channel = connection.channel()

    channel.queue_declare(queue='add_image', durable=True)  # to make sure that the queue will survive a RabbitMQ node restart
    
    def callback(ch, method, props, body):
        id, msg = body.decode().split()
        file = open("/data/" + id + ".txt", "w+")
        file.write(get_image_caption(msg))
        ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(
                         delivery_mode=2,
                     ),
                     body=id)
        ch.basic_ack(delivery_tag=method.delivery_tag) # Send ack

    channel.basic_qos(prefetch_count=1)  # This uses the basic.qos protocol method to tell RabbitMQ not to give more than one message to a worker at a time
    channel.basic_consume(queue='add_image', on_message_callback=callback)

    channel.start_consuming()