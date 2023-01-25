"""
Tester Class for creating, deleting, moving and renaming files automatically,
for testing the project logic.
"""
import os
import shutil
import time
import random
import logging
from config_parser import get_configuration


SOURCE_DIR = str(get_configuration("tester_source_dir"))
DESTINATION_DIR = str(get_configuration("watcher_source_dir"))
DEFAULT_SLEEP_TIME = get_configuration("tester_processing_time")


class Tester:

    def __init__(self):
        """
        Class Constructor.
        """
        log_file = str(get_configuration("tester_file_name", "logger"))
        log_file_mode = str(get_configuration("file_mode", "logger"))
        log_format = str(get_configuration("log_format", "logger"))
        date_format = str(get_configuration("date_format", "logger"))
        logging.basicConfig(filename=log_file, filemode=log_file_mode, level=logging.INFO,
                            format=log_format, datefmt=date_format)

    @staticmethod
    def create_file(file, content):
        """
        Method for creating new file.
        :param file: For the file name.
        :param content: For the file content.
        """
        try:
            with open(DESTINATION_DIR + '/' + str(file), 'w') as f:
                f.write(content)
            print(f"   - Creating file '{file}'.")
            time.sleep(DEFAULT_SLEEP_TIME)
            logging.info(f"File '{file}' has been created successfully.")
        except Exception as err:
            logging.error(f"Unable to create file '{file}', Error: {err}.")

    @staticmethod
    def delete_file(file):
        """
        Method for deleting a given file.
        :param file: For the file to delete.
        """
        try:
            os.remove(str(file))
            print(f"   - Deleting file '{file}'.")
            time.sleep(DEFAULT_SLEEP_TIME)
            logging.info(f"File '{file}' has been deleted successfully.")
        except (FileNotFoundError, FileExistsError) as err:
            logging.error(f"Unable to delete file '{file}', Error: {err}.")

    @staticmethod
    def rename_file(file, new_name):
        """
        Method for renaming a given file.
        :param file: For the file to rename.
        :param new_name: For the new file name.
        """
        try:
            os.rename(DESTINATION_DIR + '/' + file, DESTINATION_DIR + '/' + new_name)
            print(f"   - Renaming file '{file}' to '{new_name}'.")
            time.sleep(DEFAULT_SLEEP_TIME)
            logging.info(f"File '{file}' has been renamed to '{new_name}' successfully.")
        except (FileNotFoundError, FileExistsError) as err:
            logging.error(f"Unable to rename file '{file}', Error: {err}.")

    @staticmethod
    def move_file(file, new_path):
        """
        Method for moving a given file.
        :param file: For the file to move.
        :param new_path: For the path to move the file to.
        """
        try:
            shutil.move(file, new_path)
            print(f"   - Moving file '{file}' to '{new_path}'.")
            time.sleep(DEFAULT_SLEEP_TIME)
            logging.info(f"File '{file}' has been moved to '{new_path} successfully.'")
        except (shutil.Error, FileNotFoundError, FileExistsError) as err:
            logging.error(f"Unable to move file '{file}', Error: {err}.")

    @staticmethod
    def copy_file(file, destination):
        try:
            shutil.copy(file, destination)
            print(f"   - Copying file '{file}' to '{destination}'.")
            time.sleep(DEFAULT_SLEEP_TIME)
            logging.info(f"File '{file}' has been copied to '{destination}' successfully.")
        except (FileNotFoundError, FileExistsError) as err:
            logging.error(f"Unable to copy file '{file}', Error: {err}.")

    def run_tester(self):
        print("[+] Tester has started...")
        print(f"[+] Testing files creation...")
        # For checking files creation with duplicates
        file1 = "test.txt"
        file2 = "test.txt"
        file3 = "test2.txt"
        self.create_file(file1, "im a txt file")
        self.create_file(file2, "im a txt file")
        self.create_file(file3, "im a txt file2")
        self.create_file("test.pdf", "im a pdf file")
        self.create_file("test.pdf", "im a pdf file")
        self.create_file("test2.pdf", "im a pdf file2")
        self.create_file("test.ppt", "im a ppt file")
        self.create_file("test.ppt", "im a ppt file")
        self.create_file("test2.ppt", "im a ppt file2")

        # For checking file rename scenarios
        print(f"[+] Testing files renaming...")
        self.rename_file(file1, 'rename.txt')
        self.rename_file(file2, 'rename.txt')
        self.rename_file(file3, 'rename2.txt')

        # For checking unsupported file types
        print(f"[+] Testing files unsupported types...")
        self.create_file("unsupported.1", "im an unsupported file type.")
        self.create_file("unsupported.sadf", "im an unsupported file type1.")
        self.create_file("unsupported.info", "im an unsupported file type2.")

        # For checking download or moved or modified scenarios on supported files format
        files = os.listdir(SOURCE_DIR)
        print(f"[+] Testing files moving or modified...")
        for file in files:
            path = os.path.join(SOURCE_DIR, file)
            self.copy_file(path, DESTINATION_DIR)

        # For checking deletion of files scenarios
        files = os.listdir(DESTINATION_DIR)
        print(f"[+] Testing files deletion...")
        for file in files:
            path = os.path.join(DESTINATION_DIR, file)
            self.delete_file(path)

        # Copy again a random number of files to check same file hash scenarios
        print(f"[+] Testing files duplicates...")
        for file in random.sample(files, 4):
            path = os.path.join(SOURCE_DIR, file)
            self.move_file(path, DESTINATION_DIR)


def tester_main():

    tester = Tester()
    tester.run_tester()


tester_main()



