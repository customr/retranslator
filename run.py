import threading

from copy import deepcopy

from db_worker import *


def receiver():
	with closing(pymysql.connect(**CONN)) as connection:
		ipport_tuple = get_ipports(connection)
		TCPConnections.connect_many(ipport_tuple)
		connected = [x for x in TCPConnections.CONNECTED.keys()]

		with connection.cursor() as cursor:
			query = f'SELECT MAX(`id`) FROM `{RECORDS_TBL}`'
			cursor.execute(query)
			from_id = cursor.fetchone()['MAX(`id`)']

		while True:
			if len(TCPConnections.CONNECTED.keys())>len(connected):
				new = list(set(TCPConnections.CONNECTED.keys())-set(connected))

				for tracker in new:
					t_ip, t_port = tracker.split(':')
					connected = TCPConnections.CONNECTED.keys()
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
							RETRANSLATORS[ret_name].add_template("authentication", imei=str(row['imei']), time=int(time.time()))
							packet = RETRANSLATORS[ret_name].packet
							RETRANSLATORS[ret_name].packet = {}
							#if TCPConnections.send(tracker['ip'], tracker['port'], packet):
							#	break
							
						packet = RETRANSLATORS[ret_name].pack_record(**record)
						#if TCPConnections.send(tracker['ip'], tracker['port'], packet):
						#	break
						msg = "Отправлена запись\n"
						msg += f"ip={tracker['ip']}\nport={tracker['port']}\n"
						msg += f"ret_type={ret_name}\nrow_id={row['id']}\n"
						logger.info(msg)
						
						query = f"INSERT INTO `sent_id` VALUES ({row['id']}, '{tracker['ip']}', {tracker['port']})"
						with connection.cursor() as cursor:
							cursor.execute(query)
							connection.commit()

			rec_que.task_done()


if __name__=="__main__":
	recv_th = threading.Thread(target=receiver).start()
	send_th = threading.Thread(target=sender).start()