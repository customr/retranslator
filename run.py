import threading

from db_worker import *


def receiver():
	with closing(pymysql.connect(**CONN)) as connection:
		try:
			ipport_tuple = get_ipports(connection)
			TCPConnections.connect_many(ipport_tuple)

			all_imei = []
			connected = []
			total_count = 0
			for conn in TCPConnections.CONNECTED.keys():
				ip, port = conn.split(':')
				total_count += check_records(connection, ip, port)
				connected.append(conn)
				a_imei = get_all_imei(connection, ip, port)
				all_imei.extend(a_imei)

			logger.info(f"ИТОГО - {total_count} старых записей будут отправлены\n")

			with connection.cursor() as cursor:
				query = f'SELECT MAX(`id`) FROM `{RECORDS_TBL}`'
				cursor.execute(query)
				from_id = cursor.fetchone()['MAX(`id`)']

			send_th.start()
			while True:
				if len(TCPConnections.CONNECTED.keys())>len(connected):
					new = list(set(TCPConnections.CONNECTED.keys())-set(connected))

					for tracker in new:
						t_ip, t_port = tracker.split(':')
						connected = [x for x in TCPConnections.CONNECTED.keys()]
						check_records(connection, t_ip, t_port)
						new_imei = get_all_imei(connection, t_ip, t_port)
						all_imei.extend(new_imei)
				
				elif len(TCPConnections.CONNECTED.keys())<len(connected):
					connected = [x for x in TCPConnections.CONNECTED.keys()]

				with connection.cursor() as cursor:
					query = f"SELECT * FROM {RECORDS_TBL} WHERE `id`>{from_id} AND (`imei`={all_imei[0]}"
					for imei in all_imei[1:]:
						query += f' OR `imei`={imei}'

					query += ')'
					query += " ORDER BY `imei`, `datetime`"
					cursor.execute(query)
					
					not_emp = 0
					for row in cursor.fetchall():
						if row['lat'] is not None:
							rec_que.put(row)
							not_emp += 1
							
						from_id = row['id']
					
				
					len_s = len(TCPConnections.NOT_CONNECTED)+len(TCPConnections.CONNECTED)
					
					if not_emp:
						m = f'Найдено {not_emp} новых записей\n'
						m += f'Серверов подключено [{len(TCPConnections.CONNECTED)}/{len_s}]\n'
						logger.info(m)
					
					else:
						m = f'Новых записей не найдено\n'
						m += f'Серверов подключено [{len(TCPConnections.CONNECTED)}/{len_s}]\n'
						logger.info(m)
					
				connection.commit()
				time.sleep(DELAY)
		
		except Exception as e:
			logger.critical(f"RECEIVER {e}\n")


def sender():
	with closing(pymysql.connect(**CONN)) as connection:
		try:
			while True:
				row = rec_que.get()
				send_row(connection, row)			
				rec_que.task_done()
		
		except Exception as e:
			logger.critical(f"SENDER {e}\n")


def check_log_size():
	while True:
		if (os.path.getsize('src/logs/history.log')/1024) > 1024:
			open('src/logs/history.log', 'w').close()
		
		time.sleep(60*60*5)


recv_th = threading.Thread(target=receiver).start()
send_th = threading.Thread(target=sender)
cls_th  = threading.Thread(target=check_log_size).start()