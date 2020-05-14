import logging


frt = logging.Formatter('%(levelname)s :: %(message)s %(asctime)s\n'+'-'*75)

handler = logging.FileHandler('src/logs/history.log', mode='a')
handler.setFormatter(frt)

logger = logging.getLogger('history')
logger.addHandler(handler)
logger.setLevel(logging.INFO)