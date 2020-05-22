import logging
from logging.handlers import RotatingFileHandler


frt = logging.Formatter('%(levelname)s :: %(message)s %(asctime)s\n'+'-'*15)

handler = RotatingFileHandler(
	'src/logs/history.log.0', mode='a', 
	maxBytes=1024*1024, backupCount=2, 
	encoding=None, delay=0
	)

handler.setFormatter(frt)

logger = logging.getLogger('history')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# console = logging.StreamHandler()
# console.setLevel(logging.INFO)
# console.setFormatter(frt)
# logger = logging.getLogger('').addHandler(console)