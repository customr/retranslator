import logging
import sys
from logging.handlers import RotatingFileHandler


sys.stderr = open('src/logs/history.log', 'a')

LEVEL = logging.INFO
frt = logging.Formatter('%(levelname)s :: %(message)s %(asctime)s\n'+'-'*15)

logger = logging.getLogger()
logger.setLevel(LEVEL)
	
handler = RotatingFileHandler('src/logs/history.log', mode='a', maxBytes=10*1024*1024)
handler.setFormatter(frt)
logger.addHandler(handler)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(frt)
logger.addHandler(handler)


logger.info('RETRANSLATOR START\n\n\n\n')