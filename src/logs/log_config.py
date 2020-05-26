import logging
import sys
from logging.handlers import RotatingFileHandler


LOG_TYPE = 'file'#file или console
LEVEL = logging.INFO
frt = logging.Formatter('%(levelname)s :: %(message)s %(asctime)s\n'+'-'*15)

if LOG_TYPE=='file':
	# sys.stdout = open('src/logs/history.log', 'a')
	# sys.stderr = open('src/logs/history.log', 'a')
	handler = RotatingFileHandler('src/logs/history.log', mode='a', maxBytes=10*1024*1024)
	handler.setFormatter(frt)

elif LOG_TYPE=='console':
	handler = logging.StreamHandler(sys.stdout)
	handler.setFormatter(frt)

else:
	raise ValueError('Указан неправильный тип создания лога')

logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(LEVEL)

logger.info('RETRANSLATOR START\n\n\n\n')