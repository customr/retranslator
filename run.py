import threading

from db_worker import *


def receiver():
	with closing(pymysql.connect(**CONN)) as connection:
		ipport_tuple = get_ipports(connection)
		TCPConnections.connect_many(ipport_tuple)

		connected = []
		total_count = 0
		for conn in TCPConnections.CONNECTED.keys():
			ip, port = conn.split(':')
			total_count += check_records(connection, ip, port)
			connected.append(conn)

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

			with connection.cursor() as cursor:
				query = f"SELECT * FROM {RECORDS_TBL} WHERE `id`>{from_id}"
				cursor.execute(query)

				logger.info(f'Найдено {cursor.rowcount} новых записей\n')
				for row in cursor.fetchall():
					if (row['lat'] is not None): 
						rec_que.put(row)
						from_id = row['id']

			connection.commit()
			time.sleep(DELAY)


def sender():
	with closing(pymysql.connect(**CONN)) as connection:
		while True:
			row = rec_que.get()
			
			row['reserve'] = loads('{'+row['reserve']+'}')
			row.update(row['reserve'])
			del(row['reserve'])
			row['datetime'] = int(row['datetime'].timestamp())
			record = {n:row[n] for n in COLUMNS}

			for ret_name in RETRANSLATORS.keys():
				if not row.get('ip', ''):
					with connection.cursor() as cursor:
						query = f'SELECT * FROM {ret_name.lower()} WHERE `imei`={row["imei"]}'
						cursor.execute(query)
						trackers = cursor.fetchall()

				else:
					trackers = [{"ip":row['ip'], "port":row["port"]}]

				for tracker in trackers:
					if TCPConnections.CONNECTED.get(f"{tracker['ip']}:{tracker['port']}", ''):
						if ret_name=='EgtsRetranslator':
							if not RETRANSLATORS[ret_name].authorized_imei==str(row['imei']):
								RETRANSLATORS[ret_name].add_template("authentication", imei=str(row['imei']), time=int(time.time()))
								packet = RETRANSLATORS[ret_name].packet
								# if TCPConnections.send(tracker['ip'], tracker['port'], packet):
								# 	break
									
								RETRANSLATORS[ret_name].packet = {}

						packet = RETRANSLATORS[ret_name].pack_record(**record)
						# if TCPConnections.send(tracker['ip'], tracker['port'], packet):
						# 	break

						msg = "Отправлена запись\n"
						msg += f"ip={tracker['ip']}\nport={tracker['port']}\n"
						msg += f"ret_type={ret_name}\nrow_id={row['id']}\n"
						logger.info(msg)
						
						query = f"INSERT INTO `sent_id` VALUES ({row['id']}, '{tracker['ip']}', {tracker['port']})"
						with connection.cursor() as cursor:
							cursor.execute(query)
							connection.commit()

			rec_que.task_done()


recv_th = threading.Thread(target=receiver).start()
send_th = threading.Thread(target=sender)