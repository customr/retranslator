import os
import struct
import socket

from binascii import hexlify
from time import time, sleep, mktime, strptime
from json import load

from src.log_config import logger


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
		if lvl=='info':
			logger.info(msg + f"\n[{self.dst_ip}:{self.dst_port}] ")
		elif lvl=='critical':
			print(msg)
			logger.critical(msg + f"\n[{self.dst_ip}:{self.dst_port}] ")
		elif lvl=='error':
			print(msg)
			logger.error(msg + f"\n[{self.dst_ip}:{self.dst_port}] ")
		elif lvl=='warning':
			print(msg)
			logger.warning(msg + f"\n[{self.dst_ip}:{self.dst_port}] ")


	def __del__(self):
		self.socket.shutdown(socket.SHUT_RDWR)
		self.make_log("info", "Соединение разорвано")
		self.socket.close()	


class Retranslator(TCPConnection):

	PROTOCOLS_DIR = "src/protocols/" #место где лежат json'ы с описанием протоколов

	def __init__(self, protocol_name:str, ip:str, port:int):
		"""Родитель всех протоколов

		protocol_name (str): имя протокола, как в папке PROTOCOLS_DIR без 
		ip (str): адрес сервера
		port (int): порт сервера
		header_is_ready (bool): флаг, есть ли уже header в пакете, или нет
		packet (bytes): пакет данных
		packet_format (str): struct-формат всего пакета
		packer_params (list): параметры в соответствии с packet_format
		protocol (dict): подгруженные данные по протоколу из json

		"""
		assert isinstance(protocol_name, str)

		super().__init__(ip, port)
		self.ip = ip
		self.port = port
		self.protocol_name = protocol_name
		self.packet = bytes()
		self.data = {}

		self.get_protocol()
		self.make_log("info", f"Протокол {protocol_name} инициализирован")


	def get_protocol(self):
		path = os.path.join(self.PROTOCOLS_DIR, self.protocol_name+".json")
		assert os.path.exists(path), "Неправильно указан путь до протокола"

		with open(path, 'r') as s:
			self.protocol = load(s)


	def send(self, endiannes='>'):
		self.make_log("info", f"Началась отправка пакета данных")
		result_code = super().send(self.packet)
		self.reset()
		return result_code


	def reset(self):
		self.packet = bytes()
		self.make_log("info", "Ретранслятор очищен от данных")


	def __str__(self):
		return f"{self.protocol_name} [{self.ip}:{self.port}]"


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


	def paste_data_into_params(self, params, data, formats):
		for n in params.keys():
			if isinstance(params[n], str):
				slc = ('[' in params[n])
				if "*"==params[n][0]:
					other_format = False
					params[n] = params[n][1:]

					if slc:
						params[n], slc = params[n].split("[")
						slc = slc[:-1]

						if formats[n]!=formats[params[n]]:
							other_format = True

					if params[n] in data.keys():
						params[n] = data[params[n]]
					else:
						try:
							params[n] = getattr(self, params[n])
						except AttributeError:
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
	def in_correct_order(data_format:dict, data:dict):
		"""
		Сортирует параметры в нужном порядке.
		На вход получаем словарь, на выход массив

		data_format (dict): словарь с именами параметров и их типом данных
		data (dict): исходные параметры
		"""

		ordered_data = []
		for key, fmt in data_format.items():
			fmt = ''.join([i for i in fmt if not i.isdigit() and not i in 'x?'])
			if fmt in 'hHiIqQnN':
				ordered_data.append(data.get(key, 0))
			elif fmt in 'efd':
				ordered_data.append(data.get(key, 0.0))
			elif fmt in 'cbBsp':
				ordered_data.append(data.get(key, b''))
			elif fmt=='?':
				ordered_data.append(data.get(key, False))
			else:
				error_msg = f"Ошибка в типе данных '{params[n]} : {fmt}'"
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
			xlen = fmt.rfind('?')
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