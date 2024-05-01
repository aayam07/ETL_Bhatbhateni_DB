import logging
logging.basicConfig(filename="main.log", filemode='a', format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S", level=logging.WARNING)

def log_info(info):
    logging.warning(info)

def log_error(err):
    logging.error(err)

def log_warning(warning_msg):
    logging.warning(warning_msg)