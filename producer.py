"""
Producer Class for publish file changes events to RabbitMQ queue.
"""
import pika
import pika.exceptions
from time import sleep
from config_parser import get_configuration


class Producer:

    def __init__(self, host: str):
        """
        Class Constructor.
        :param host: For the hot ip address.
        """
        self.host = host
        self.queue = str(get_configuration("rabbitmq_queue_name"))
        self.connection = None
        self.channel = None
        self.RECONNECTING_BUFFER = get_configuration("reconnecting_buffer")
        self.connect()

    def connect(self) -> None:
        """
        Establish connection to RabbitMQ Server.
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue)
        print(f"[+] Producer connected successfully to RabbitMQ queue '{self.queue}'.")

    def reconnect(self) -> None:
        """
        Reconnects to RabbitMQ Server for a given number of tries.
        """
        attempts = get_configuration("reconnect_retries")
        for attempt in range(int(attempts)):
            print(f"[-] Producer reconnection attempt #{attempt + 1}")
            sleep(int(self.RECONNECTING_BUFFER))
            self.connect()
            if self.connection:
                break
            else:
                print(f"[!] Failed to reconnect to RabbitMQ server.")
                self.close_connection()

    def close_connection(self) -> None:
        """
        Closes the RabbitMQ connection.
        """
        self.connection.close()

    def publish(self, msg: str) -> None:
        """
        Publish a given msg to RabbitMQ queue.
        :param msg: For the msg to publish.
        """
        self.channel.basic_publish(exchange='', routing_key=self.queue, body=msg)
