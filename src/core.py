import os
import struct
import socket

from binascii import hexlify
from time import time, sleep, mktime, strptime
from json import load
from copy import deepcopy

from src.logs.log_config import logger


DATE_FORMAT = "%Y-%m-%d %H:%M:%S" #формат даты и времени (как в базе данных)


class TCPConnection:

	CONNECTION_ATTEMPTS = 40 #максимальное количество попыток соединиться с сервером
	ATTEMPTS_DELAY = 10 #начальное кол-во секунд паузы

	def __init__(self, dst_ip:str, dst_port:int):
		"""Обеспечивает общение по TCP протоколу

		dst_ip (str): ip адрес получателся
		dst_port (int): порт получаетеля
		"""
		assert isinstance(dst_ip, str), 'Неправильно указан хост'
		assert isinstance(dst_port, int), 'Неправильно указан порт'

		self.dst_ip = dst_ip
		self.dst_port = dst_port
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.connect(self.CONNECTION_ATTEMPTS)
		self.server_answer = bytes()


	def connect(self, attempts:int):
		"""Устанавливает TCP соединение

		attemps (int): кол-во попыток соединиться с получателем
		"""
		assert attempts>0

		if attempts:
			try:
				self.socket.connect((self.dst_ip, self.dst_port))
				self.make_log("info", 'Соединение установлено')

			except Exception as e:
				self.make_log("error", f'Не удалось установить соединение. Попытка №{self.CONNECTION_ATTEMPTS-attempts} ({e})')
				sleeptime = min(self.ATTEMPTS_DELAY*(self.CONNECTION_ATTEMPTS-attempts-1), 10000)
				sleep(sleeptime)
				self.connect(attempts-1)

		else:
			self.make_log("critical", 'Невозможно установить соединение')
			self.socket.close()
			raise RuntimeError('Невозможно установить соединение')


	def send(self, bmsg:bytes):
		"""Отправляет сообщение получателю

		bmsg (bytes): сообщение в байтах
		"""

		#FIX: socket.recv блокирует поток, если ответа от сервера не поступило

		assert isinstance(bmsg, bytes), 'Пакет данных дожен быть в байтовом формате'
		try:
			msglen = len(bmsg)
			totalsent = 0
			while totalsent < msglen:
				sent = self.socket.send(bmsg[totalsent:])
				if sent==0:
					raise RuntimeError()
				totalsent += sent

			self.make_log("info", f'Пакет данных успешно отправлен (size {msglen} bytes)\n{hexlify(bmsg)}')
			self.server_answer = self.socket.recv(1024)
			self.make_log("info", f'Ответ сервера (size {len(self.server_answer)} bytes)\n{hexlify(self.server_answer)}')
			return 0

		except Exception as e:
			self.make_log("critical", f"Ошибка при отправке данных ({e})")
			return -1


	def make_log(self, lvl:str, msg:str):
		"""Дополняет информацию в логах

		lvl (str): уровень сообщения
		msg (str): сообщение

		"""
		if lvl=='info':
			logger.info(msg + f"\n[{self.dst_ip}:{self.dst_port}] ")
		elif lvl=='debug':
			logger.debug(msg + f"\n[{self.dst_ip}:{self.dst_port}] ")
		elif lvl=='critical':
			logger.critical(msg + f"\n[{self.dst_ip}:{self.dst_port}] ")
		elif lvl=='error':
			logger.error(msg + f"\n[{self.dst_ip}:{self.dst_port}] ")
		elif lvl=='warning':
			logger.warning(msg + f"\n[{self.dst_ip}:{self.dst_port}] ")


	def __del__(self):
		self.socket.shutdown(socket.SHUT_RDWR)
		self.make_log("info", "Соединение разорвано")
		self.socket.close()	


class Retranslator(TCPConnection):

	PROTOCOLS_DIR = "src/protocols/" #место где лежат json'ы с описанием протоколов
	DATA_DIR = "src/protocols/data/" #место под данные

	def __init__(self, protocol_name:str, ip:str, port:int):
		"""Родитель всех протоколов

		protocol_name (str): имя протокола, как в папке PROTOCOLS_DIR без 
		ip (str): адрес сервера
		port (int): порт сервера
		header_is_ready (bool): флаг, есть ли уже header в пакете, или нет
		packet (bytes): пакет данных
		packet_format (str): struct-формат всего пакета
		packer_params (list): параметры в соответствии с packet_format
		data (dict): хранит введенные параметры шаблонов
		protocol (dict): подгруженные данные по протоколу из json

		"""
		assert isinstance(protocol_name, str)

		super().__init__(ip, port)
		self.ip = ip
		self.port = port
		self.protocol_name = protocol_name

		self.packet = bytes()
		self.data = {}

		self.protocol = Retranslator.get_json(self.PROTOCOLS_DIR, self.protocol_name+".json")

		p = os.path.join(self.DATA_DIR, self.protocol_name)
		if not os.path.exists(p):
			os.makedirs(p)

		p = os.path.join(os.path.join(self.DATA_DIR, self.protocol_name), self.protocol_name+'.id')
		if not os.path.exists(p):
			open(p, 'w').close()

		self.make_log("info", f"Протокол {protocol_name} инициализирован")


	def send(self, uid=0):
		"""
		Перегруженный родительский метод отправки сообщения

		uid (int): уникальный идентификатор записи бд
		"""

		self.make_log("info", f"Началась отправка пакета данных")
		result_code = super().send(self.packet)
		if result_code:
			self.make_log("error", 'Потеряно соединение с сервером')
			if uid:
				pth = os.path.join(self.protocol["DATA_PATH"], self.protocol_name+'.id')
				with open(pth, 'w') as w:
					w.write(uid)

				self.make_log("warning", f"Сохранен идентификатор последней записи {uid}")

			self.socket.shutdown(socket.SHUT_RDWR)
			self.socket.close()
			self.connect(self.CONNECTION_ATTEMPTS)
			result_code = super().send(self.packet)

			if result_code:
				self.make_log('critical', 'Пакет вызывает ошибку на сервере')
				raise RuntimeError('Пакет вызывает ошибку на сервере')

			else:
				open(pth, 'w').close()

		self.reset()
		return result_code


	def reset(self):
		"""
		Восстанавливает класс в исходное состояние
		"""
		self.packet = bytes()
		self.make_log("debug", "Ретранслятор очищен от данных")


	def paste_data_into_params(self, p, data, formats):
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

						if find_format(n)!=find_format(params[n]):
							other_format = True #формат исходной переменной несовпадает с требуемым форматом

					if params[n] in data.keys():
						params[n] = data[params[n]]

					else:
						error_msg = f"Параметр '{params[n]}' не найден"
						self.make_log("critical", error_msg)
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
							self.make_log("critical", error_msg)
							raise KeyError(error_msg)

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
							self.make_log("critical", error_msg)
							raise ValueError(error_msg)

			else:
				continue

		return params


	@staticmethod
	def processing(fmt:dict, params:dict, endiannes=">"):
		"""Обработчик
		
		Расставляет параметры в нужном порядке
		Преобразует формат и параметры, вставляя нужные данные
		Пакует данные в байты

		fmt (dict): в виде param_name:struct_fmt
		params (dict): в виде param_name:value
		endiannes (str): byte-order
		"""
		params = Retranslator.in_correct_order(fmt, params)
		fmt, params = Retranslator.handler(''.join(fmt.values()), params)
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

		if ('d' in fmt) or ('D' in fmt):
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

		if not os.path.exists(p):
			open(p, 'w').close()

		else:
			with open(p, 'r') as s:
				file = load(s)

			return file


	def __str__(self):
		return f"{self.protocol_name} [{self.ip}:{self.port}]"