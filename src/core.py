import os
import struct
import socket

from time import time, sleep
from json import load

from src.log_config import logger


class TCPConnection:

	CONNECTION_ATTEMPTS = 5 #количество попыток соединиться с сервером
	ATTEMPT_DELAY = 5 #пауза между попытками в секундах

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


	def connect(self, attempts):
		"""Устанавливает TCP соединение

		attemps (int): кол-во попыток соединиться с получателем
		"""
		assert attempts>0

		if attempts:
			try:
				self.socket.connect((self.dst_ip, self.dst_port))
				self.make_log("info", 'Соединение установлено')

			except Exception as e:
				self.make_log("error", f'Не удалось установить соединение ({e})')
				sleep(self.ATTEMPT_DELAY)
				self.connect(attempts-1)

		else:
			self.make_log("critical", 'Невозможно установить соединение')
			self.socket.close()
			raise RuntimeError()


	def send(self, bmsg):
		"""Отправляет сообщение получателю

		bmsg (bytes): сообщение в байтах
		"""
		assert isinstance(bmsg, bytes), 'Пакет данных дожен быть в байтовом формате'

		try:
			self.socket.send(bmsg)
			self.make_log("info", f'Пакет данных успешно отправлен (size {len(bmsg)} bytes)')
			return 0

		except Exception as e:
			self.make_log("critical", f"Ошибка при отправке данных ({e})")
			return -1


	def make_log(self, lvl, msg):
		if lvl=='info':
			logger.info(f"[{self.dst_ip}:{self.dst_port}] "+msg)
		elif lvl=='critical':
			logger.critical(f"[{self.dst_ip}:{self.dst_port}] "+msg)
		elif lvl=='error':
			logger.error(f"[{self.dst_ip}:{self.dst_port}] "+msg)
		elif lvl=='warning':
			logger.warning(f"[{self.dst_ip}:{self.dst_port}] "+msg)


	def __del__(self):
		self.socket.shutdown(socket.SHUT_RDWR)
		self.make_log("info", "Соединение разорвано")
		self.socket.close()	


class Retranslator(TCPConnection):

	PROTOCOLS_DIR = "src/protocols/" #место где лежат json'ы с описанием протокола

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
		self.header_is_ready = False
		self.packet = bytes()
		self.packet_format = ""
		self.packet_params = []

		self.get_protocol()
		self.make_log("info", "Протокол инициализирован")


	def get_protocol(self):
		path = os.path.join(self.PROTOCOLS_DIR, self.protocol_name+".json")
		assert os.path.exists(path), "Неправильно указан путь до протокола"

		with open(path, 'r') as s:
			self.protocol = load(s)


	def add_x(self, name, **params):
		"""
		Добавляет часть пакета описанную в json

		name (str): имя блока (в blocks_format и blockheader_data)
		params (kwargs): параметры в соотвествии с выбранным блоком 
		"""
		
		data = self.in_correct_order(name, params) #тасуем параметры в порядке, указанном в протоколе

		#добавляем формат описания блока
		blockheader_format = ''.join(self.protocol["blockheader"].values())
		self.packet_format += blockheader_format

		#тут же вставляем эти параметры
		blockheader_data = self.protocol["blockheader_data"][name].values()
		self.packet_params.extend(blockheader_data)

		if self.protocol["blocks_format"].get(name, None):
			#добавляем формат для данных
			data_format = ''.join(self.protocol["blocks_format"][name].values())
			self.packet_format += data_format
		
		#и тут же их вставляем
		self.packet_params.extend(data)
		self.make_log("info", f"Добавлен блок '{name}'")


	def in_correct_order(self, name, data):
		"""
		Сортирует параметры в нужном порядке.
		На вход получаем словарь, на выход массив

		name (str): имя в json в blocks_format
		data (dict): исходные параметры
		"""

		if self.protocol["blocks_format"].get(name, None):
			ordered_data = []
			for key in self.protocol["blocks_format"][name].keys():
				ordered_data.append(data[key])

			return ordered_data

		else:
			return data.values()


	def send(self):
		self.packet_format, self.packet_params = self.handler(self.packet_format, self.packet_params)
		self.packet += struct.pack(">"+self.packet_format, *self.packet_params)
		self.make_log("info", "Пакет с данными готов к отправке")
		super().send(self.packet)


	@staticmethod
	def handler(fmt:str, params):
		"""
		в struct.pack есть небольшая недоработка:
		нельзя указать неизвестное количество символов в строке,
		если строка будет меньше указанной длины, например в формате "30s",
		а у нас строка длиной 18, то остальные 12 байт заполнятся ненужными для нас нулями
		эта фунция позволяет in-time вставить туда длину строки

		fmt (str): формат (см. документацию struct.pack)
		params (list): параметры 
		"""

		#для удобства можем str переформатировать в bytes obj

		known_str = []
		for n, param in enumerate(params):
			if isinstance(param, str):
				params[n] = bytes(param.encode('utf-8'))
				known_str.append(params[n])

		#знак вопроса заменяем длиной строки
		while fmt.find('?')!=-1:
			xlen = fmt.find('?')
			left, right = fmt[:xlen], fmt[xlen+1:]
			fmt = left + str(len(known_str[fmt.count("s",0,xlen)])) + right

		return fmt, params


	def __str__(self):
		return f"{self.protocol_name} [{self.ip}:{self.port}]"