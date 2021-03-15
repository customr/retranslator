import pymysql
import time
import os
import socket

from copy import deepcopy
from contextlib import closing
from json import loads
from queue import Queue
from datetime import datetime

from src.core import TCPConnections
from src.retranslators import Wialon, EGTS, WialonIPS, GalileoSky, EGTSNoAuth
from src.logs.log_config import logger
from db_connect import *

RETRANSLATORS_ALL = (
	'WialonIPS',
	'Egts',
	'Wialon',
	'GalileoSky',
	'EGTSNoAuth'
)

RETRANSLATORS = {
	'Egts'		: 	EGTS(),
	'Wialon'	: 	Wialon(),
	'WialonIPS'	:	WialonIPS(),
	'GalileoSky': 	GalileoSky(),
	'EGTSNoAuth':	EGTSNoAuth()
}

RETRANSLATOR_IDS = {ret.lower():n for ret, n in zip(RETRANSLATORS_ALL, range(1, len(RETRANSLATORS_ALL)+1))}


class Tracker:

	CONN_DELAY = 1

	def __init__(self, dbconn, imei, retranslator, ip, port):
		self.ip = ip
		self.port = port
		self.imei = imei
		self.dbconn = dbconn
		self.retranslator = retranslator
		self.retranslator_id = RETRANSLATOR_IDS[self.protocol.protocol_name.lower()]
		self.settings = get_settings(self.dbconn, self.retranslator.protocol_name.lower(), self.imei)

		self.queue = Queue()
		self.socket = -1

		send_th = threading.Thread(target=self.sender)
		send_th.start()

	def get_settings(self):
		query = f"SELECT `settings` FROM `retranslate_settings` WHERE `protocol`={self.retranslator_id} AND `imei`={int(self.imei)}"
		with self.dbconn.cursor() as cursor:
			cursor.execute(query)
			if cursor.rowcount!=0:
				settings = loads(cursor.fetchone()['settings'])
			else:
				settings = {}

		return settings

	def connect(self):
		sock = socket.socket()
		sock.settimeout(1)
		try:
			sock.connect((ip, port))
			return sock

		except Exception as e:
			logger.debug(f'Не удалось установить соединение ({e})'+ f"\n{self.imei} [{ip}:{port}] ")
			return -1

	def fill_queue(self):
		with self.dbconn.cursor() as cursor:
			query = f"SELECT MAX(`id`) FROM `sent_id` WHERE `ip`='{self.ip}' AND `port`={self.port} AND `imei`={self.imei}"
			cursor.execute(query)
			last_id = cursor.fetchone()['MAX(`id`)']
			if last_id == None:
				query = f"SELECT MAX(`id`) FROM `{RECORDS_TBL}`"
				cursor.execute(query)
				last_id = cursor.fetchone()['MAX(`id`)']

				query = f"INSERT INTO `sent_id` VALUES ({last_id}, '{self.ip}', {self.port}, {self.imei})"
				cursor.execute(query)
				self.dbconn.commit()
			
			query = f"SELECT * FROM {RECORDS_TBL} WHERE `id`>{last_id} AND `imei`={self.imei}"
			cursor.execute(query)
			rows = cursor.fetchall()

			notemp = 0
			for row in rows:
				if (row.get('lat', None) is not None) and (row['datetime'].timestamp()>0):
					self.queue.put(row)
					notemp += 1
			
			if not ret:
				logger.debug(f'Найдено {notemp} записей для {self.imei} [{self.ip}:{self.port}]\n')

	def sender(self):
		while True:
			while self.socket==-1:
				self.socket = self.connect()
				sleep(CONN_DELAY)

			while self.queue.qsize()==0:
				self.fill_queue()

			row = self.queue.get()

			if row.get('reserve', None):
				row['imei'] = str(row['imei'])
				row['reserve'] = loads('{'+row['reserve']+'}')
				row.update(row['reserve'])
				del(row['reserve'])
				
				if not row.get('sat_num', ''):
					row.update({"sat_num":0})
				
				row['lon'] = float(row['lon'])
				row['lat'] = float(row['lat'])

			sended, status = self.retranslator.send(self.send, row, self.settings)

			if sended:
				msg = "Запись ОТПРАВЛЕНА\n"
			else: 
				msg = "Запись НЕ ОТПРАВЛЕНА\n"
			
			msg += "Сервер".ljust(26, '-')+f"{self.ip}:"+f"{self.port}\n"
			msg += "Ретранслятор".ljust(26, '-')+f"{self.retranslator.protocol_name}\n"
			msg += "ID записи".ljust(26, '-')+f"{row['id']}\n"
			msg += "imei".ljust(26, '-')+f"{row['imei']}\n"
			msg += "Время точки".ljust(26, '-')+f"{datetime.fromtimestamp(row['datetime'])}\n"
			msg += "Статус отправки".ljust(26, '-')+f"{status}\n"
			msg += f"Записей для {self.imei}".ljust(26, '-')+f"{self.queue.qsize()}\n"
			
			if not sended:
				logger.error(msg)
			else:
				logger.info(msg)
				condition = f" WHERE `ip`='{ip}' AND `port`={port} AND `imei`={row['imei']}"
				query = f"SELECT * FROM `sent_id`" + condition
				with self.dbconn.cursor() as cursor:
					cursor.execute(query)
					if cursor.rowcount==0:
						query = f"INSERT INTO `sent_id` VALUES ({row['id']}, '{self.ip}', {self.port}, {row['imei']})"
					else:
						query = f"UPDATE `sent_id` SET `id`={row['id']}"+condition
					
					cursor.execute(query)
					self.dbconn.commit()

			self.queue.task_done()

	def send(bmsg):
		try:
			msglen = len(bmsg)

			self.socket.send(bmsg)
			server_answer = self.socket.recv(1024)
			logger.debug(f'Пакет данных успешно отправлен (size {msglen} bytes)\n{hexlify(bmsg)}\n'+f'Ответ сервера (size {len(server_answer)} bytes)\n{hexlify(server_answer)}'+ f"\n{self.imei} [{self.ip}:{self.port}] ")
			return hexlify(server_answer)

		except Exception as e:
			self.socket.close()
			self.socket = -1
			logger.critical(f"Ошибка при отправке данных ({e})"+ f"\n{self.imei} [{ip}:{port}] ")
			return -1


def get_all_imei(connection, ip=None, port=None):
	all_imei = []
	all_imei_by_ret = {}
	for ret_name in RETRANSLATORS_ALL:
		with connection.cursor() as cursor:
			if ip and port:
				query = f"SELECT `imei` FROM `{RET_TABLE}` WHERE `protocol`={RETRANSLATOR_IDS[ret_name.lower()]} AND `ip`='{ip}' AND `port`={port}"
			else:
				query = f"SELECT `imei` FROM `{RET_TABLE}` where `protocol`={RETRANSLATOR_IDS[ret_name.lower()]}"
				
			cursor.execute(query)
			all_imei_by_ret[ret_name] = []
			
			for i in cursor.fetchall():
				data = {
					"imei": i['imei'],
					"ip": ip,
					"port": port
				}
				all_imei.append(data)
				all_imei_by_ret[ret_name].append(data)

	return all_imei, all_imei_by_ret
	

def get_retranslator(ip, port):
	for ret, ipports in ret_by_ipport.items():
		if (ip, int(port)) in ipports:
			return ret

