import logging

success_logger = logging.getLogger("producer_success")
success_logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("producer_success.log",mode='w')
formatter = logging.Formatter('%(asctime)s - %(message)s')
file_handler.setFormatter(formatter)

success_logger.addHandler(file_handler)
