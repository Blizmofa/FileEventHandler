"""
File Change Handler Class for watch the wanted folder for file changes.
"""
import pika
import pika.exceptions
from typing import Union
from producer import Producer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent


class FileChangeWatcher(FileSystemEventHandler):

    def __init__(self, host: str):
        """
        Class Constructor.
        """
        self.producer = Producer(host)
        self.file_paths = []

    def on_any_event(self, event: Union[FileCreatedEvent]):
        """
        Method to send to RabbitMQ queue the file change event.
        :param event: For the event to send.
        """
        # Avoid directory changes
        if event.is_directory:
            return None

        # Add the file creation path to path lists
        if isinstance(event, FileCreatedEvent):
            self.file_paths.append(event.src_path)

        # Send event type and file path to RabbitMQ queue for further processing
        msg = f"{event.event_type} {event.src_path}"
        try:
            self.producer.publish(msg)
        except (pika.exceptions.ConnectionClosed, pika.exceptions.StreamLostError, AttributeError) as err:
            print(f"[!] Unable to send event to RabbitMQ, Error: {err}, Trying to reconnect...")
            # In case connection will be terminated
            self.producer.reconnect()
            self.producer.publish(msg)


