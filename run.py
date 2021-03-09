import threading

from db_worker import *
from db_connect import *

def receiver():
	with closing(pymysql.connect(**CONN)) as connection:
		tstart = time.time()
		ipport_tuple = get_ipports(connection)
		TCPConnections.connect_many(ipport_tuple)
		from_id = {}

		connected = []
		total_count = 0
		connected_now = [x for x in TCPConnections.CONNECTED.keys()]
		
		for conn in connected_now:
			ip, port = conn.split(':')
			c = check_records(connection, ip, port)
			total_count += c

		logger.info(f"ИТОГО - {total_count} старых записей будут отправлены\n")

		with connection.cursor() as cursor:
			query = f'SELECT MAX(`id`) FROM `{RECORDS_TBL}`'
			cursor.execute(query)
			mid = cursor.fetchone()['MAX(`id`)']
			from_id = {ret: deepcopy(mid) for ret in RETRANSLATORS_ALL}

		for ipport in rec_que.keys():
			threading.Thread(target=sender, args=(ipport, )).start()
			
		while True:
			for ret_name in RETRANSLATORS_ALL:
				receive_rows(connection, ret_name, tstart)
				connection.commit()
			
			time.sleep(DELAY)


def sender(ipport):
	with closing(pymysql.connect(**CONN)) as connection:
		ip, port = ipport.split(':')
		retranslator = get_retranslator(ip, port)
		while True:
			row = rec_que[ipport].get()
			send_row(connection, row, retranslator, True)
			rec_que[ipport].task_done()


def check_log_size():
	while True:
		if (os.path.getsize('src/logs/history.log')/1024) > 1024*10*10:
			open('src/logs/history.log', 'w').close()
		
		if (os.path.getsize('src/logs/debug.log')/1024) > 1024*10*10:
			open('src/logs/debug.log', 'w').close()

		time.sleep(60*60*4)


recv_th = threading.Thread(target=receiver).start()
cls_th  = threading.Thread(target=check_log_size).start()
