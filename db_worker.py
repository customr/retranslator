import pymysql
import time
import os

from contextlib import closing
from json import loads
from queue import Queue
from datetime import datetime, timezone

from src.core import TCPConnections
from src.retranslators import WialonRetranslator, EGTS
from src.logs.log_config import logger


HOST 		= 	'127.0.0.1'
PORT 		=   3306
USER 		= 	'root'
PASSWD 		=	'root'
DB 			= 	'devices'

RECORDS_TBL = 	'geo_100'  	 #имя таблицы, в которую поступают записи
DELAY 		= 	1.0   	 	 #через сколько секунд проверять бд на наличие новых записей

CONN 		= 	{
				"host"		 : 	HOST,
				"port"		 :  PORT,
				"user"		 : 	USER,
				"password"	 : 	PASSWD,
				"db"		 : 	DB,
				"charset"	 : 	'utf8mb4',
				"cursorclass": 	pymysql.cursors.DictCursor
				}

RETRANSLATORS = {
				'EgtsRetranslator'	: 	EGTS(),
				'WialonRetranslator': 	WialonRetranslator(),
				}


rec_que = Queue()

def get_ipports(connection):
	ipport_tuple = []
	for ret_name in RETRANSLATORS.keys():
		with connection.cursor() as cursor:
			query = f"SELECT DISTINCT ip, port FROM {ret_name.lower()}"
			cursor.execute(query)
			logger.info(f'{cursor.rowcount} соединений для {ret_name}\n')
			for row in cursor:
				if (row['ip'], int(row['port'])) not in ipport_tuple:
					ipport_tuple.append((row['ip'], int(row['port'])))

	return ipport_tuple


def check_records(connection, ip, port):
	def check_records_for_imei(imei):
		with connection.cursor() as cursor:
			query = f"SELECT MAX(`id`) FROM `sent_id` WHERE `ip`='{ip}' AND `port`={port} AND `imei`={imei}"
			cursor.execute(query)
			last_id = cursor.fetchone()['MAX(`id`)']
			if last_id == None:
				query = f"SELECT MAX(`id`) FROM `{RECORDS_TBL}`"
				cursor.execute(query)
				last_id = cursor.fetchone()['MAX(`id`)']

				query = f"INSERT INTO `sent_id` VALUES ({last_id}, '{ip}', {port}, {imei})"
				cursor.execute(query)
				connection.commit()

		with connection.cursor() as cursor:
			query = f"SELECT * FROM {RECORDS_TBL} WHERE `id`>{last_id} AND `imei`={imei}"
			query += " ORDER BY `datetime`"
			cursor.execute(query)

			logger.info(f'Найдено {cursor.rowcount} записей для {imei} [{ip}:{port}]\n')
			for row in cursor.fetchall():
				if (row['lat'] is not None):
					row.update({"ip":ip, "port":port}) 
					rec_que.put(row)

			rowcount = cursor.rowcount

		return rowcount

	all_imei = get_all_imei(connection, ip, port)
	count = 0
	for imei in all_imei:
		count += check_records_for_imei(imei)

	return count


def get_all_imei(connection, ip, port):
	all_imei = []
	for ret_name in RETRANSLATORS.keys():
		with connection.cursor() as cursor:
			query = f"SELECT `imei` FROM {ret_name.lower()} WHERE `ip`='{ip}' AND `port`={port}"
			cursor.execute(query)

			for i in cursor.fetchall():
				all_imei.append(i['imei'])

	return all_imei
	

def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def send_row(connection, row):
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
			tn = f"{tracker['ip']}:{tracker['port']}"
			if TCPConnections.CONNECTED.get(tn, ''):
				#sended = RETRANSLATORS[ret_name].send(tracker['ip'], tracker['port'], row)
				sended = 1
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
					condition = f" WHERE `ip`='{tracker['ip']}' AND `port`={tracker['port']}"
					condition += f" AND `imei`={row['imei']}"

					query = f"SELECT * FROM `sent_id`" + condition
					with connection.cursor() as cursor:
						cursor.execute(query)
						if cursor.rowcount==0:
							query = f"INSERT INTO `sent_id` VALUES ({row['id']}, '{row['ip']}', {row['port']}, {row['imei']})"
						else:
							query = f"UPDATE `sent_id` SET `id`={row['id']}"+condition
						
						cursor.execute(query)
						connection.commit()
