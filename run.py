import threading

from db_worker import *


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

		for th in send_th:
			th.start()
			
		while True:
			for ret_name in RETRANSLATORS_ALL:
				if rec_que[ret_name].qsize()==0:
					receive_rows(connection, ret_name, tstart)
					connection.commit()
			
			time.sleep(10)


def sender(ret, th):
	workers = 10
	with closing(pymysql.connect(**CONN)) as connection:
		while True:
			row = rec_que[ret].get()
			retranslator = RETRANSLATORS[ret]
			send_row(connection, row, retranslator, True)
			rec_que[ret].task_done()


def check_log_size():
	while True:
		if (os.path.getsize('src/logs/history.log')/1024) > 1024*10*10:
			open('src/logs/history.log', 'w').close()
		
		time.sleep(60*60*4)


#with closing(pymysql.connect(**CONN)) as connection:
#	ipports = get_ipports(connection)
#	c_per_th = 8
#	servers_th = []
#	
#	for i in range(len(ipports)//c_per_th):
#		servers_th.append(ipports[i*c_per_th:i*c_per_th+c_per_th])
#	
#	servers_th.append(ipports[len(ipports)//c_per_th*c_per_th+1:])
#	assoc = {}
#	
#	for n, th in enumerate(servers_th):
#		for ipport in th:
#			assoc.update({f"{ipport[0]}:{ipport[1]}": n})
#	
#	print(assoc)

recv_th = threading.Thread(target=receiver).start()
cls_th  = threading.Thread(target=check_log_size).start()

send_th = []
send_th.append(threading.Thread(target=sender, args=('EgtsRetranslator',0,)))
send_th.append(threading.Thread(target=sender, args=('WialonRetranslator',1,)))
send_th.append(threading.Thread(target=sender, args=('WialonIPSRetranslator',2,)))
