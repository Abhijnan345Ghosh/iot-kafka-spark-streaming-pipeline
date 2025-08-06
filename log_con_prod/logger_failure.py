import logging

failure_logger = logging.getLogger("producer_failure")
failure_logger.setLevel(logging.ERROR)

file_handler = logging.FileHandler("producer_failure.log",mode='w')
formatter = logging.Formatter('%(asctime)s - %(message)s')
file_handler.setFormatter(formatter)

failure_logger.addHandler(file_handler)
