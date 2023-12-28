import logging
import pathlib

BASE_DIR = pathlib.Path(__file__).parents[0]

def setup_logger(name):
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    file_handler = logging.FileHandler(
        BASE_DIR / 'logs.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

if __name__ == "__main__":
    print(BASE_DIR)