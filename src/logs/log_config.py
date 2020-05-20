import logging
from logging.handlers import RotatingFileHandler


frt = logging.Formatter('%(levelname)s :: %(message)s %(asctime)s\n'+'-'*75)

handler = RotatingFileHandler(
	'src/logs/history.log', mode='a', 
	maxBytes=1024*1024, backupCount=2, 
	encoding=None, delay=0
	)

handler.setFormatter(frt)

logger = logging.getLogger('history')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# console = logging.StreamHandler()
# console.setLevel(logging.INFO)
# console.setFormatter(frt)
# logger = logging.getLogger('').addHandler(console)