import threading

from db_worker import *
from db_connect import *

TRACKERS = []

def check_log_size():
	while True:
		if (os.path.getsize('src/logs/history.log')/1024) > 1024*10*10:
			open('src/logs/history.log', 'w').close()
		
		if (os.path.getsize('src/logs/debug.log')/1024) > 1024*10*10:
			open('src/logs/debug.log', 'w').close()

		time.sleep(60*60*4)


cls_th  = threading.Thread(target=check_log_size).start()

with closing(pymysql.connect(**CONN)) as connection:
	for t in get_trackers(connection):
		ret = RETRANSLATOR_NAMES[t['protocol']]
		tracker = Tracker(connection, t['imei'], ret, t['ip'], t['port'])
		TRACKERS.append(tracker)