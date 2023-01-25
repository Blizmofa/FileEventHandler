"""
Project main method to run the FileHandler.
"""
from threading import Thread
from handler import FileHandler, HandlerError


def main():

    fh = FileHandler('localhost')
    try:
        file_handler_thread = Thread(target=fh.run)
        file_handler_thread.start()
    except HandlerError as err:
        print(err)
        fh.stop_observer()


if __name__ == "__main__":
    main()
