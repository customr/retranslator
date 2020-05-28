import pymysql
import time

from contextlib import closing
from json import loads
from queue import Queue

from src.core import TCPConnections
from src.retranslators import WialonRetranslator, EGTS
from src.logs.log_config import logger


HOST 		= 	'127.0.0.1'
PORT 		=   3306
USER 		= 	'retranslator'
PASSWD 		=	'cMiOm1rZ'
DB 			= 	'tracks'

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
	with connection.cursor() as cursor:
		query = f"SELECT MAX(`id`) FROM `sent_id` WHERE `ip`='{ip}' AND `port`={port}"
		cursor.execute(query)
		last_id = cursor.fetchone()['MAX(`id`)']
		if last_id == None: last_id = 0

	all_imei = get_all_imei(connection, ip, port)

	with connection.cursor() as cursor:
		query = f"SELECT * FROM {RECORDS_TBL} WHERE `id`>{last_id} AND (`imei`={all_imei[0]}"
		for imei in all_imei[1:]:
			query += f' OR `imei`={imei}'

		query += ')'
		cursor.execute(query)

		logger.info(f'Найдено {cursor.rowcount} записей для [{ip}:{port}]\n')
		for row in cursor.fetchall():
			if (row['lat'] is not None):
				row.update({"ip":ip, "port":port}) 
				rec_que.put(row)

		rowcount = cursor.rowcount
		
	return rowcount


def get_all_imei(connection, ip, port):
	all_imei = []
	for ret_name in RETRANSLATORS.keys():
		with connection.cursor() as cursor:
			query = f"SELECT `imei` FROM {ret_name.lower()} WHERE `ip`='{ip}' AND `port`={port}"
			cursor.execute(query)

			for i in cursor.fetchall():
				all_imei.append(i['imei'])

	return all_imei


def get_rec_from_to(connection, imei, from_dt, to_dt):
	with connection.cursor() as cursor:
		query = f"SELECT * FROM `{RECORDS_TBL}` WHERE `imei`='{imei}'"
		query += f" AND `datetime`>'{from_dt}' AND `datetime`<'{to_dt}'"
		cursor.execute(query)
		return cursor.fetchall()