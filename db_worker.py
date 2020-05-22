import os
import pymysql

from contextlib import closing
from json import loads
from time import sleep

from src.core import TCPConnection
from src.retranslators import WialonRetranslator, EGTS
from src.logs.log_config import logger


HOST 		= 	'localhost'
USER 		= 	'root'
PASSWD 		=	'root'
DB 			= 	'devices'

RECORDS_TBL = 	'geo'

ID_PATH 	= 	"src/id"
DELAY 		= 	10

CONN 		= 	{
				"host"		 : 	HOST,
				"user"		 : 	USER,
				"password"	 : 	PASSWD,
				"db"		 : 	DB,
				"charset"	 : 	'utf8mb4',
				"cursorclass": 	pymysql.cursors.DictCursor
				}

RETRANSLATORS = {
				'egts'				: 	EGTS(),
				'wialonretranslator': 	WialonRetranslator(),
				}

COLUMNS 	= 	(
				'id', 
				'imei', 
				'lat', 
				'lon', 
				'datetime', 
				'speed', 
				'direction',
				'ignition',
				'sensor',
				'sat_num'
				)


def get_connections(connection):
	connections = {}
	for ret_name in RETRANSLATORS.keys():
		connections[ret_name] = []
		with connection.cursor() as cursor:
			query = f"SELECT * FROM {ret_name}"
			cursor.execute(query)
			for row in cursor:
				conn = TCPConnection(row['ip'], row['port'])
				connections[ret_name].append(conn)

	return connections


def get_records(connection):
	path = open(ID_PATH, 'r')
	r = path.readline()
	if r:
		last_id = int(r)
	else:
		raise ValueError('id не установлен!')

	path.close()

	records = []
	with connection.cursor() as cursor:
		query = f"SELECT * FROM {RECORDS_TBL} WHERE id > {last_id}"
		while True:
			cursor.execute(query)
			if cursor.rowcount!=0: break
			logger.info(f'Новых записей не найдено. Жду {DELAY} секунд')
			sleep(DELAY)

		for row in cursor:
			if (row['lat'] is not None):
				row['reserve'] = loads('{'+row['reserve']+'}')
				row.update(row['reserve'])
				del(row['reserve'])
				row['datetime'] = int(row['datetime'].timestamp())
				record = {n:row[n] for n in COLUMNS}
				records.append(record)

			else:
				continue

	return records
