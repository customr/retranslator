import os
import struct
import socket
import threading

from binascii import hexlify
from time import time, sleep, mktime, strptime
from json import load
from copy import deepcopy

from src.logs.log_config import logger


DATE_FORMAT = "%Y-%m-%d %H:%M:%S" #формат даты и времени (как в базе данных)


class TCPConnections:

	ATTEMPTS_DELAY = 15 #начальное кол-во секунд паузы между попытками присоединиться
	CONNECTED = {}
	NOT_CONNECTED = []

	@staticmethod
	def connect_many(ipport_tuple):
		for ip, port in ipport_tuple:
			if TCPConnections.connect(ip, port)==-1:
				TCPConnections.NOT_CONNECTED.append((ip, port))

		if len(TCPConnections.NOT_CONNECTED):
			msg = f'Не удалось установить соединение с [{len(TCPConnections.NOT_CONNECTED)}/{len(ipport_tuple)}] серверами:\n- '
			msg += '\n- '.join([f"{ip}:{port}" for ip, port in TCPConnections.NOT_CONNECTED])+'\n'
			logger.error(msg)

		connect_th = threading.Thread(target=TCPConnections.retry_connect)
		connect_th.start()


	@staticmethod
	def retry_connect():
		while True:
			try:
				n = 0
				for ip, port in TCPConnections.NOT_CONNECTED:
					if TCPConnections.connect(ip, int(port))!=-1:
						del(TCPConnections.NOT_CONNECTED[n])

					n += 1

				sleep(30)
			
			except Exception as e:
				logger.error(f"Ошибка в функции переподключения ({e})")
		
		
	@staticmethod
	def close_conn(ip, port):
		ind = f"{ip}:{port}"
		try:
			TCPConnections.CONNECTED[ind].shutdown(socket.SHUT_RDWR)
			TCPConnections.CONNECTED[ind].close()
			del(TCPConnections.CONNECTED[ind])
		
		except Exception as e:
			logger.error(f"Не удалось закрыть соединение ({e})")


	@staticmethod
	def connect(ip, port):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.settimeout(2)

		try:
			sock.connect((ip, port))
			TCPConnections.CONNECTED.update({f'{ip}:{port}':sock})
			logger.info('Соединение установлено'+ f"\n[{ip}:{port}] ")
			return sock

		except Exception as e:
			logger.debug(f'Не удалось установить соединение ({e})'+ f"\n[{ip}:{port}] ")
			return -1


	@staticmethod
	def send(ip, port, bmsg:bytes):
		"""Отправляет сообщение получателю

		bmsg (bytes): сообщение в байтах
		"""

		assert isinstance(bmsg, bytes), 'Пакет данных дожен быть в байтовом формате'

		sock = TCPConnections.CONNECTED.get(f'{ip}:{port}', '')
		if sock:
			try:
				msglen = len(bmsg)

				sock.send(bmsg)
				server_answer = sock.recv(1024)
				logger.debug(f'Пакет данных успешно отправлен (size {msglen} bytes)\n{hexlify(bmsg)}'+ f"\n[{ip}:{port}] ")
				logger.debug(f'Ответ сервера (size {len(server_answer)} bytes)\n{hexlify(server_answer)}'+ f"\n[{ip}:{port}] ")
				return hexlify(server_answer)
	

			except Exception as e:
				logger.critical(f"Ошибка при отправке данных ({e})"+ f"\n[{ip}:{port}] ")
				TCPConnections.close_conn(ip, port)
				TCPConnections.NOT_CONNECTED.append((ip, port))
				logger.info(f"Сервер [{ip}:{port}] добавлен в неподключенные")
				return -1

		else:
			logger.error('Попытка отправить данные на неподключенный сервер'+ f"\n[{ip}:{port}] ")
			return -1


	def __repr__(self):
		return f'TCPConnections (count={len(TCPConnections.CONNECTED.keys())})'

	def __str__(self):
		return self.__repr__()


class Retranslator:

	PROTOCOLS_DIR = "src/protocols/" #место где лежат json'ы с описанием протоколов

	def __init__(self, protocol_name:str):
		"""Родитель всех протоколов

		protocol_name (str): имя протокола, как в папке PROTOCOLS_DIR без расширения
		header_is_ready (bool): флаг, есть ли уже header в пакете, или нет
		packet (bytes): пакет данных
		packet_format (str): struct-формат всего пакета
		packer_params (list): параметры в соответствии с packet_format
		data (dict): хранит введенные параметры шаблонов
		protocol (dict): подгруженные данные по протоколу из json

		"""
		assert isinstance(protocol_name, str)
		self.protocol_name = protocol_name

		self.packet = bytes()
		self.data = {}

		self.protocol = Retranslator.get_json(self.PROTOCOLS_DIR, self.protocol_name+".json")
		logger.info(f"Протокол {protocol_name} инициализирован\n")


	def paste_data_into_params(self, p, data, formats=None):
		"""
		Знак "&" в значении параметра json означает ссылку на переменную
		после этого знака следует написать имя параметра, который вы передали в класс
		и этот параметр возьмет значение этой переменной

		Также здесь есть срезы, делаются они точно так же, как и в питоне, но нет шагов.

		Например: "param": "&myvar[3:7]"
		
		Args:
			p (dict): параметры, в которых необходимо заменить специальные выражения переменными
			data (dict): словарь, из которого берутся переменные для параметров
			formats (dict): форматы struct для параметров (p)
			
		Returns:
			dict: параметры со вставленными значениями

		"""

		def find_format(name):
			"""Глобальный поиск формата в протоколе
			name (str): имя параметра
			"""
			for key, item in self.protocol["FORMATS"].items():
				if name in item.keys():
					return item[name]

		params = deepcopy(p) #глубокая копия чтобы избежать недоразумений
		for n in params.keys():
			if isinstance(params[n], str):
				slc = ('[' in params[n])
				if "&"==params[n][0]:
					other_format = False 
					params[n] = params[n][1:]

					if slc:
						params[n], slc = params[n].split("[")
						slc = slc[:-1]
						
						if formats:
							if find_format(n)!=find_format(params[n]):
								other_format = True #формат исходной переменной несовпадает с требуемым форматом

					if params[n] in data.keys():
						params[n] = data[params[n]]

					else:
						error_msg = f"Параметр '{params[n]}' не найден"
						logger.critical(error_msg)
						raise KeyError(error_msg)

					if slc:
						l_slc, r_slc = slc.split(':')
						if ((len(l_slc)>0) & (len(r_slc)>0)):
							params[n] = params[n][int(l_slc):int(r_slc)]
						elif ((len(l_slc)>0) & (not len(r_slc)>0)):
							params[n] = params[n][int(l_slc):]
						elif ((not len(l_slc))>0 & (len(r_slc)>0)):
							params[n] = params[n][:int(r_slc)]
						else:
							error_msg = f"Неправильно указан срез параметра {params[n]}[{l_slc}:{r_slc}]"
							logger.critical(error_msg)
							raise KeyError(error_msg)
					
					if formats:
						if other_format:
							fmt = formats[n]
							#согласно обозначениям типов struct
							if fmt in 'hHiIqQnN':
								params[n] = int(params[n])
							elif fmt in 'efd':
								params[n] = float(params[n])
							elif fmt in 'cbBsp':
								params[n] = bytes(str(params[n]).encode('ascii'))
							else:
								error_msg = f"Ошибка в типе данных '{params[n]} : {fmt}'"
								logger.critical(error_msg)
								raise ValueError(error_msg)

			else:
				continue

		return params


	@staticmethod
	def processing(fmt:dict, params:dict, endiannes=">"):
		"""Обработчик
		
		Преобразует формат и параметры, вставляя нужные данные
		Пакует данные в байты

		fmt (dict): в виде param_name:struct_fmt
		params (dict): в виде param_name:value
		endiannes (str): byte-order
		"""
		params = Retranslator.in_correct_order(fmt, params)
		fmt, params = Retranslator.handler(''.join(fmt.values()), params)
		logger.debug(f"{fmt} {params}")
		block = Retranslator.pack_data(fmt, params, endiannes)
		return block


	@staticmethod
	def in_correct_order(data_format:dict, data:dict):
		"""
		Сортирует параметры в нужном порядке.
		На вход получаем словарь, на выход массив

		data_format (dict): словарь с именами параметров и их типом данных
		data (dict): исходные параметры
		"""

		ordered_data = []
		for key, fmt in data_format.items():
			fmt = ''.join([i for i in fmt if not i.isdigit() and not i in 'x?='])
			if fmt in 'hHiIqQnN':
				ordered_data.append(data.get(key, 0))
			elif fmt in 'efd':
				ordered_data.append(data.get(key, 0.0))
			elif fmt in 'cbBsp':
				ordered_data.append(data.get(key, b''))
			elif fmt=='?':
				ordered_data.append(data.get(key, False))
			else:
				error_msg = f"Ошибка в типе данных '{data_format[key]} : {fmt}'"
				logger.critical(error_msg)
				raise ValueError(error_msg)

		return ordered_data


	@staticmethod
	def pack_data(fmt:str, params:list, endiannes=">"):
		""" Запаковщик данных

		fmt (str): формат всего пакета (struct)
		params (list): все параметры пакета
		endiannes (list): byte-order (по умолчанию big-endian)
		"""

		packet = bytes()

		if (('d' in fmt) or ('D' in fmt)) and len(fmt)>1:
			doubles = []
			f_parts = fmt.split("d")

			for param in params:
				if isinstance(param, float):
					doubles.append(param)

			for n, part in enumerate(f_parts[:-1]):
				ind = params.index(doubles[n])

				packet += struct.pack(endiannes+part, *params[:ind])
				packet += struct.pack("<d", doubles[n])

				del(params[:ind+1])

			packet += struct.pack(endiannes+f_parts[-1], *params)

		else:
			try:
				packet += struct.pack(endiannes+fmt, *params)

			except Exception as e:
				logger.critical(f'Ошибка в запаковке данных {fmt} - {params} ({e})')
				raise e

		return packet


	@staticmethod
	def handler(fmt:str, params:list):
		"""
		в struct.pack есть небольшая недоработка:
		нельзя указать неизвестное количество символов в строке,
		если строка будет меньше указанной длины, например в формате "30s",
		а у нас строка длиной 18, то остальные 12 байт заполнятся ненужными для нас нулями
		эта фунция позволяет in-time вставить туда длину строки

		Например: "param": "=myvar" 
		param будет равен длине строки myvar

		fmt (str): формат (см. документацию struct.pack)
		params (list): параметры 
		"""

		known_str = []
		for n in range(len(params)):
			if isinstance(params[n], str):
				params[n] = bytes(params[n].encode('utf-8'))
				known_str.append(params[n])

		#знак равенства заменяем длиной строки
		while fmt.find('=')!=-1:
			xlen = fmt.rfind('=')
			left, right = fmt[:xlen], fmt[xlen+1:]
			fmt = left + str(len(known_str.pop())) + right

		return fmt, params


	@staticmethod
	def get_timestamp(date_time:str):
		"""Преобразует дату и время в timestamp
		
		date_time (str): дата и время согласно константе DATE_FORMAT
		"""
		if isinstance(date_time, str):
			return int(mktime(strptime(date_time, DATE_FORMAT)))
		else:
			logger.critical("Неизвестный формат даты и времени")
			raise ValueError("Неизвестный формат времени")


	@staticmethod
	def get_json(dr, name):
		"""
		Возвращает данные из файла

		dr (str): путь до файла
		name (str): имя файла + расширение
		"""
		if not os.path.exists(dr):
			os.makedirs(dr)

		p = os.path.join(dr, name)
		with open(p, 'r') as s:
			file = load(s)

		return file


	def __str__(self):
		return f"Protocol {self.protocol_name}"


	def __repr__(self):
		return self.__str__()