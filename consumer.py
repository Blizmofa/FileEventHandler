"""
Consumer Class for running the main project logic, see readme for more details.
"""
import pathlib
import sys
from time import sleep
import hashlib
import os
import pika
import pika.exceptions
import enum
from threading import Thread
from logger import Logger
from database import DB, CreateTableError, UpdateError, DeleteError
from config_parser import get_configuration


class Consumer(Thread):
    def __init__(self, host: str):
        """
        Class Constructor.
        :param host: For the IP Address to configure.
        """
        super(Consumer).__init__()
        self.host = host
        self.queue = str(get_configuration("rabbitmq_queue_name"))
        self.connection = None
        self.channel = None
        self.file_types = [".ppt", ".pptx", ".pdf", ".txt", ".html", ".mp4",
                           ".jpg", ".png", ".xls", ".xlsx", ".xml", ".vsd", ".py",
                           ".doc", ".docx", ".json"]
        self.chunk_size = get_configuration("chunk_size")
        self.RECONNECTING_BUFFER = get_configuration("reconnecting_buffer")
        self.DEFAULT_PROCESSING_TIME = get_configuration("default_processing_time")
        self.hash = hashlib.md5()
        self.db = DB(str(get_configuration("consumer_database_name")))
        self.class_logger = Logger('Consumer')

    def connect(self) -> None:
        """
        Establish connection to RabbitMQ Server.
        """
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(self.queue)
            print(f"[+] Consumer connected successfully to RabbitMQ queue '{self.queue}'.")
            self.class_logger.logger.info(f"Consumer connected successfully to RabbitMQ queue '{self.queue}'.")
        except Exception as err:
            print(f"[!] Unable to connect to RabbitMQ Server due to {err}.")

    def close_connection(self) -> None:
        """
        Closes connection to rabbitMQ Server.
        """
        self.connection.close()
        print(f"[+] Consumer connection has been closed.")

    def reconnect(self) -> None:
        """
        Reconnects to RabbitMQ Server for a given number of tries.
        """
        attempts = get_configuration("reconnect_retries")
        for attempt in range(attempts):
            print(f"[-] Consumer reconnection attempt #{attempt + 1}")
            sleep(self.RECONNECTING_BUFFER)
            self.connect()
            if self.connection.is_open:
                break
            else:
                print(f"[!] Failed to reconnect to RabbitMQ server.")
                self.class_logger.logger.error("Failed to reconnect to RabbitMQ server for {attempts} times.")
                self.close_connection()

    def consume(self) -> None:
        """
        Starts the consumer.
        """
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.on_notification_receive)
        print(f"[+] Consumer is now listening to RabbitMQ queue '{self.queue}'...")
        try:
            self.channel.start_consuming()
        except Exception as err:
            print(f"[!] Unable to consume, Error: {err}")

    def setup_consumer_db(self) -> None:
        """
        Method For setting up the consumer database.
        """
        try:
            self.db.create_table('Files', 'File_Name, File_Hash')
        except CreateTableError as err:
            print(err)
            sys.exit(1)

    def on_notification_receive(self, channel, method, properties, body):
        """
        This method will do the following on the received events:
        1. if 'created':
          - generate file md5 hash,
          - if file hash already in db the consumer will change file name and add the appropriate suffix,
          - otherwise stores the hash into consumer db.
        2. if 'deleted':
          - delete file from db.
        3. if 'moved' or 'modified':
          - save to log file.
        :param channel: For RabbitMQ channel.
        :param method: For RabbitMQ delivery method.
        :param properties: For RabbitMQ properties.
        :param body: For received event message.
        """
        file_name = None
        file_hash = None
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        decoded_msg = body.decode().split()

        # Getting file path and hash
        try:
            file_name = decoded_msg[1]
            file_hash = self.hash_file(file_name)
        except FileNotFoundError as err:
            self.class_logger.logger.error(f"[!] Unable to get file path or hash, Error: {err}")

        # Validating file type
        file_type = self.validate_file_type(file_name)

        # Main method logic
        if file_type:
            # For create event
            if EventTypes.CREATED in decoded_msg:
                # Getting file size to calculate consumer processing time
                size = self.get_file_size_in_bytes(file_name)
                processing_time = self.get_file_process_time(size)
                print(f"[+] Received created event, processing time will be {processing_time} seconds.")
                # Insert hash value only if it does not exist in db
                if self.db.insert_if_not_exists('Files', 'File_Hash', file_hash):
                    try:
                        self.db.update_table('Files', 'File_Name', file_name, 'File_Hash', file_hash)
                    except UpdateError as err:
                        print(f"[!] Unable to update database, Error: {err}.")
                # If md5 hash already exists in db, change file name
                else:
                    try:
                        new_name = f"{file_name}{'_dup_#'}"
                        os.rename(file_name, new_name)
                        self.class_logger.logger.debug(f"Changed {file_name} to {new_name}")
                    except FileNotFoundError as err:
                        self.class_logger.logger.error(f"Unable to rename {file_name}, Error: {err}")
                sleep(processing_time)
            # For delete event
            elif EventTypes.DELETED in decoded_msg:
                print(f"[+] Received deleted event, processing time will be {self.DEFAULT_PROCESSING_TIME} seconds.")
                try:
                    # file_hash = self.db.select_value('Files', 'File_Hash')
                    self.db.delete_value('Files', 'File_Name', file_name)
                    self.db.delete_value('Files', 'File_Name', file_hash)
                    # sleep(processing_time)
                except DeleteError as err:
                    print(f"[!] Unable to delete '{file_hash}' from db, Error: {err}.")
            # For moved or modified event
            elif EventTypes.MOVED in decoded_msg or EventTypes.MODIFIED in decoded_msg:
                print(f"[+] Received modified or moved event, processing time will be {self.DEFAULT_PROCESSING_TIME} seconds.")
                self.class_logger.logger.debug(f"Received '{decoded_msg}'.")

    def run(self):
        """
        Method to run the consumer with reconnecting ability.
        """
        self.setup_consumer_db()
        if self.connection.is_closed or self.channel.is_closed:
            print(f"[!] Unable to connect, check RabbitMQ Server status.")
        else:
            try:
                self.consume()
            except Exception as err:
                print(f"[!] Connection closed due to {err}, Trying to reconnect...")
                self.class_logger.logger.error(f"Connection to RabbitMQ Server forcibly closed, Error: {err}")
                # In case connection will be terminated
                self.reconnect()
                self.consume()

    def hash_file(self, file: str) -> str:
        """
        Generating md5 hash for a given file.
        :param file: For the file to hash.
        :return: The given file md5 hash code.
        """
        try:
            file_to_hash = open(file, 'rb')
            # For file first block
            chunk = file_to_hash.read(int(self.chunk_size))
            # Read until EOF
            while chunk:
                self.hash.update(chunk)
                chunk = file_to_hash.read(int(self.chunk_size))
            # Returns the file hash
            hash_result = self.hash.hexdigest()
            self.class_logger.logger.debug(f"File '{file}' md5 hash is: '{hash_result}'.")
            return hash_result
        except FileNotFoundError as err:
            self.class_logger.logger.error(f"Unable to generate md5 hash for '{file}', Error: {err}")

    def validate_file_type(self, file: str) -> bool:
        """
        Auxiliary method for validating file type according to supported file types list.
        :param file: For the file to validate.
        :return: True if file type is supported, False otherwise.
        """
        file_type = pathlib.Path(file).suffix
        if file_type in self.file_types:
            self.class_logger.logger.debug(f"File type '{file_type}' is supported.")
            return True
        else:
            self.class_logger.logger.debug(f"File type '{file_type}' is NOT supported.")
            return False

    def get_file_size_in_bytes(self, file: str) -> int:
        """
        Auxiliary method for getting the file size in bytes.
        :param file: For the given file to check.
        :return: The file size in bytes.
        """
        try:
            file_size = os.path.getsize(file)
            self.class_logger.logger.debug(f"File '{file}' size is: {file_size}")
            return file_size
        except FileNotFoundError as err:
            self.class_logger.logger.error(f"Unable to get file '{file}' size, Error: {err}")

    @staticmethod
    def get_file_process_time(size_in_bytes: int):
        """
        Auxiliary method for converting size of bytes to units representation.
        :param size_in_bytes: For the size of bytes to calculate.
        :return: The processing time according to the unit size.
        """
        if size_in_bytes in SizeUnits.KB_RANGE.value:
            return SizeUnits.KB.value
        elif size_in_bytes in SizeUnits.MB_RANGE.value:
            return SizeUnits.MB.value
        elif size_in_bytes in SizeUnits.GB_RANGE.value:
            return SizeUnits.GB.value
        elif size_in_bytes in SizeUnits.TB_RANGE.value:
            return SizeUnits.TB.value


"""
Auxiliary class for handling event types.
"""


class EventTypes:
    CREATED = 'created'
    DELETED = 'deleted'
    MOVED = 'moved'
    MODIFIED = 'modified'


"""
Auxiliary class for file size units and ranges.
"""


class SizeUnits(enum.Enum):
    BYTES = 1
    KB = 2
    MB = 3
    GB = 4
    TB = 5
    KB_RANGE = range(0, 1024)
    MB_RANGE = range(1024, 1048576)
    GB_RANGE = range(1048576, 1073741824)
    TB_RANGE = range(1073741824, 1099511627776)
