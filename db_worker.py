import pymysql
import time
import os

from copy import deepcopy
from contextlib import closing
from json import loads
from queue import Queue
from datetime import datetime

from src.core import TCPConnections
from src.retranslators import Wialon, EGTS, WialonIPS, GalileoSky
from src.logs.log_config import logger
from db_connect import *

RETRANSLATORS_ALL = (
	'WialonIPS',
	'Egts',
	'Wialon',
	'GalileoSky'
)

RETRANSLATORS = {
	'Egts'		: 	EGTS(),
	'Wialon'	: 	Wialon(),
	'WialonIPS'	:	WialonIPS(),
	'GalileoSky': 	GalileoSky()
}

RETRANSLATOR_IDS = {ret.lower():n for ret, n in zip(RETRANSLATORS_ALL, range(len(RETRANSLATORS_ALL)))}
rec_que = {ret:Queue() for ret in RETRANSLATORS_ALL}
ignored_imei = {}



def get_ipports(connection, ret=None):
	ipport_tuple = []
	if ret:
		retall = [ret]
	else:
		retall = RETRANSLATORS_ALL
		
	for ret_name in retall:
		with connection.cursor() as cursor:
			query = f"SELECT DISTINCT ip, port FROM `{RET_TABLE}` WHERE `protocol`={RETRANSLATOR_IDS[ret_name.lower()]}"
			cursor.execute(query)
			if not ret:
				logger.info(f'{cursor.rowcount} соединений для {ret_name}\n')
				
			for row in cursor:
				if (row['ip'], int(row['port'])) not in ipport_tuple:
					ipport_tuple.append((row['ip'], int(row['port'])))
	
	return ipport_tuple


def check_records(connection, ip, port, ret=None):
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
			cursor.execute(query)

			if ret is None:
				retall = RETRANSLATORS_ALL
			
			else:
				retall = [ret]
				
			rows = cursor.fetchall()
			notemp = 0
			for row in rows:
				if (row.get('lat', None) is not None) and (row['datetime'].timestamp()>0):
					for ret_name in retall:
							if row['imei'] in imei_by_ret[ret_name]:
								rec_que[ret_name].put(row)
					
					notemp += 1
			
			if not ret:
				logger.info(f'Найдено {notemp} записей для {imei} [{ip}:{port}]\n')
			
		return notemp

	all_imei, imei_by_ret = get_all_imei(connection, ip, port)
	if not ret:
		logger.info(f"$ Всего машин для {ip}:{port} - {len(all_imei)}\n")
	count = 0
	for imei in all_imei:
		c = check_records_for_imei(imei)
		count += c
		
	return count


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
				all_imei.append(i['imei'])
				all_imei_by_ret[ret_name].append(i['imei'])

	return all_imei, all_imei_by_ret
	

def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def get_rec_from_to(connection, imei, frm, to):
	with connection.cursor() as cursor:
		query = f"SELECT * FROM {RECORDS_TBL} WHERE `imei`={imei}"
		query += f" AND `datetime`>'{frm}' AND `datetime`<'{to}' LIMIT 500000"
		cursor.execute(query)
		rows = cursor.fetchall()

	return rows


def send_row(connection, row, retranslator, update=True):
	if int(str(row['datetime'])[:4])<2010:
		return {}
		
	if row.get('reserve', None):
		row['imei'] = str(row['imei'])
		row['reserve'] = loads('{'+row['reserve']+'}')
		row.update(row['reserve'])
		del(row['reserve'])
		
		if not row.get('sat_num', ''):
			row.update({"sat_num":0})
		
		row['lon'] = float(row['lon'])
		row['lat'] = float(row['lat'])
	
	trackers = []
	with connection.cursor() as cursor:
		query = f"SELECT * FROM `{RET_TABLE}` WHERE `protocol`={RETRANSLATOR_IDS[ret_name.lower()]} AND `imei`={row['imei']}"
		cursor.execute(query)
		if cursor.rowcount>0:
			trackers = cursor.fetchall()

	sended_all = {}
	for tracker in trackers:
		tn = f"{tracker['ip']}:{tracker['port']}"
					
		if (not TCPConnections.CONNECTED.get(tn, None)) and ((tracker['ip'], tracker['port']) not in TCPConnections.NOT_CONNECTED):
			logger.info(f"Обнаружен новый сервер {tracker['ip']}:{tracker['port']}")
			if TCPConnections.connect(tracker['ip'], tracker['port'])==-1:
				logger.info(f"Подключение к нему не удалось\n")
				TCPConnections.NOT_CONNECTED.append((tracker['ip'], tracker['port']))
				
		if TCPConnections.CONNECTED.get(tn, ''):
			tm = time.time()
			
			sended, status = retranslator.send(tracker['ip'], tracker['port'], row)
			sended_all.update({tn: status})
			
			if sended:
				msg = "Запись ОТПРАВЛЕНА\n"
			else:
				msg = "Запись НЕ ОТПРАВЛЕНА\n"
			
			elapsed_time = time.time()-tm
			
				
			msg += "Сервер".ljust(26, '-')+f"{tracker['ip']}:"+f"{tracker['port']}\n"
			msg += "Ретранслятор".ljust(26, '-')+f"{retranslator.protocol_name}\n"
			msg += "ID записи".ljust(26, '-')+f"{row['id']}\n"
			msg += "imei".ljust(26, '-')+f"{row['imei']}\n"
			msg += "Время точки".ljust(26, '-')+f"{datetime.fromtimestamp(row['datetime'])}\n"
			msg += "Статус отправки".ljust(26, '-')+f"{status}\n"
			msg += "Затраченное время (сек)".ljust(26, '-')+"{:.2f}\n".format(elapsed_time)
			for ret, x in rec_que.items():
				msg += f"Записей для {ret}".ljust(26, '-')+f"{x.qsize()}\n"
			
			if not sended:
				logger.error(msg)
				continue
			
			else:
				logger.info(msg)
				if update:
					condition = f" WHERE `ip`='{tracker['ip']}' AND `port`={tracker['port']}"
					condition += f" AND `imei`={row['imei']}"
					
					query = f"SELECT * FROM `sent_id`" + condition
					with connection.cursor() as cursor:
						cursor.execute(query)
						if cursor.rowcount==0:
							query = f"INSERT INTO `sent_id` VALUES ({row['id']}, '{tracker['ip']}', {tracker['port']}, {row['imei']})"
						else:
							query = f"UPDATE `sent_id` SET `id`={row['id']}"+condition
						
						cursor.execute(query)
						connection.commit()
			
	return sended_all
	
	
def receive_rows(connection, ret_name, tstart):
	not_emp = 0
	ipports = get_ipports(connection, ret_name)
	for ip, port in ipports:
		if not TCPConnections.CONNECTED.get(f'{ip}:{port}', None):
			if (ip, port) not in TCPConnections.NOT_CONNECTED:
				TCPConnections.NOT_CONNECTED.append((ip, port))
			
			continue
			
		not_emp += check_records(connection, ip, port, ret_name)
		
	len_s = len(TCPConnections.NOT_CONNECTED)+len(TCPConnections.CONNECTED)
	
	if not_emp:
		m = f'Найдено {not_emp} новых записей\n'
	
	else:
		m = f'Новых записей не найдено\n'
	
	m += f"Протокол {ret_name}\n"
	m += f'Серверов подключено [{len(TCPConnections.CONNECTED)}/{len_s}]\n'
	m += f"Время работы: {int((time.time()-tstart)/60)} минут(ы)\n"
	logger.info(m)
	
	return not_emp