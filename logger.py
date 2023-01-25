"""
Custom logger class based on python logging library.
"""
import logging
from config_parser import get_configuration


class Logger:

    def __init__(self, logger_name: str):
        """
        Class Constructor.
        Initializes the Following:
        1. log file name, path and mode.
        2. log level.
        3. log date and time format.
        :param logger_name: For the logger name to be shown in the log file.
        """
        # Create logger
        self.logger = logging.getLogger(logger_name)
        log_file = str(get_configuration("main_file_name", "logger"))
        log_file_mode = str(get_configuration("file_mode", "logger"))
        # Set level and format
        log_level = self.set_log_level()
        self.logger.setLevel(log_level)
        log_format = str(get_configuration("log_format", "logger"))
        date_format = str(get_configuration("date_format", "logger"))
        logging.basicConfig(filename=log_file, filemode=log_file_mode, level=log_level,
                            format=log_format, datefmt=date_format)

    @staticmethod
    def set_log_level() -> int:
        """
        Sets the log level according to the config file.
        """
        log_level = str(get_configuration("debug_mode", "logger"))
        if log_level == "false".lower():
            return logging.INFO
        elif log_level == "true".lower():
            return logging.DEBUG


