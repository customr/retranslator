import threading

from datetime import datetime, timezone

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
			logger.critical(f"Receiver stopped ({e})")


def sender():
	with closing(pymysql.connect(**CONN)) as connection:
		try:
			while True:
				row = rec_que.get()
				
				row['imei'] = str(row['imei'])
				row['reserve'] = loads('{'+row['reserve']+'}')
				row.update(row['reserve'])
				del(row['reserve'])
				row['datetime'] = int(utc_to_local(row['datetime']).timestamp())
				row.update({"time":int(utc_to_local(datetime.now()).timestamp())})

				if not row.get('sat_num', ''):
					row.update({"sat_num":0})
				
				row['lon'] = float(row['lon'])
				row['lat'] = float(row['lat'])
				
				retranslators = []
				for ret_name in RETRANSLATORS.keys():
					with connection.cursor() as cursor:
						query = f'SELECT * FROM {ret_name.lower()} WHERE `imei`={row["imei"]}'
						cursor.execute(query)
						if cursor.rowcount!=0:
							retranslators.append(ret_name)
							
				for ret_name in retranslators:
					if not row.get('ip', ''):
						with connection.cursor() as cursor:
							query = f'SELECT * FROM {ret_name.lower()} WHERE `imei`={row["imei"]}'
							cursor.execute(query)
							trackers = cursor.fetchall()

					else:
						trackers = [{"ip":row['ip'], "port":row["port"]}]

					for tracker in trackers:
						if TCPConnections.CONNECTED.get(f"{tracker['ip']}:{tracker['port']}", ''):
							sended = 1
							if ret_name=='EgtsRetranslator':
								if RETRANSLATORS[ret_name].auth_imei!=str(row['imei']):
									RETRANSLATORS[ret_name].packet = bytes()
									RETRANSLATORS[ret_name].add_template("authentication", imei=str(row['imei']), time=int(time.time()))
									packet = RETRANSLATORS[ret_name].packet
									if TCPConnections.send(tracker['ip'], tracker['port'], packet):
										sended = 0
										
									RETRANSLATORS[ret_name].auth_imei = str(row['imei'])
							
							packet = RETRANSLATORS[ret_name].pack_record(**row)
							if TCPConnections.send(tracker['ip'], tracker['port'], packet):
								sended = 0
							
							if sended:
								msg = "Запись ОТПРАВЛЕНА\n"
							else:
								msg = "Запись НЕ ОТПРАВЛЕНА\n"
								
							msg += f"ip={tracker['ip']}\nport={tracker['port']}\n"
							msg += f"ret_type={ret_name}\nrow_id={row['id']}\nimei={row['imei']}\n"
							msg += f"datetime={datetime.fromtimestamp(row['datetime'])}\n"
							
							
							if not sended:
								logger.error(msg)
								continue
							
							else:
								logger.info(msg)
								query = f"INSERT INTO `sent_id` VALUES ({row['id']}, '{tracker['ip']}', {tracker['port']})"
								with connection.cursor() as cursor:
									cursor.execute(query)
									connection.commit()
							
							
				rec_que.task_done()
		
		except Exception as e:
			logger.critical(f"Sender stopped ({e})")


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)

recv_th = threading.Thread(target=receiver).start()
send_th = threading.Thread(target=sender)