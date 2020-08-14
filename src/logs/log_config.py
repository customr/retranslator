import logging
import sys

from logging.handlers import RotatingFileHandler

sys.stderr = open('src/logs/errors.log', 'a')

PATH = "src/logs/"
LEVEL = logging.DEBUG
frt = logging.Formatter('%(levelname)s :: %(message)s %(asctime)s\n'+'-'*15)

logger = logging.getLogger()
logger.setLevel(LEVEL)

handler = RotatingFileHandler(PATH+'history.log', mode='a', maxBytes=1024*1024*5)
handler.setLevel(logging.INFO)
handler.setFormatter(frt)
logger.addHandler(handler)

handler = RotatingFileHandler(PATH+'debug.log', mode='a', maxBytes=1024*1024*5)
handler.setLevel(logging.DEBUG)
handler.setFormatter(frt)
logger.addHandler(handler)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(frt)
logger.addHandler(handler)


logger.info('RETRANSLATOR START\n\n\n\n')