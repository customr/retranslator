import struct
import time

from src.core import Retranslator
from src.crc import crc8, crc16


class WialonRetranslator(Retranslator):

	FLAGS = 1 #битовая маска пакета (местоположение)
	DATATYPES = { #всевозможноые типы блока данных описанных в документации
		1: 's',
		2: 'b',
		3: 'i',
		4: 'd',
		5: 'l'
	}

	def __init__(self, ip, port):
		"""WialonRetranslator протокол
		https://gurtam.com/hw/files/Wialon%20Retranslator_v1.pdf

		ip (str): ip адрес получателся
		port (int): порт получаетеля
		"""

		super().__init__("WialonRetranslator", ip, port)


	def add_block(self, name, **params):
		"""
		Добавляет часть пакета описанную в json

		name (str): имя блока (в blocks_format и blockheader_data)
		params (kwargs): параметры в соотвествии с выбранным блоком 
		"""
		block = bytes()

		if not name=='header':
			fmt = self.protocol["FORMATS"]['blockheader']
			data = self.protocol["PARAMS"]["blockheader"][name]
			block += Retranslator.processing(fmt, data)

		fmt = self.protocol["FORMATS"].get(name, '')
		if fmt:
			block += Retranslator.processing(fmt, params)

		else:
			datatype = data.get('data_type', None)
			if datatype:
				datatype = self.DATATYPES[datatype]
				block += Retranslator.pack_data(datatype, params.values())

		self.packet += block
		self.make_log("info", f"Добавлен блок '{name}' в пакет данных")


	def send(self):
		assert len(self.packet)>0

		packet_size = len(self.packet)
		packet_size = struct.pack("<I", packet_size)
		self.packet = packet_size + self.packet
		super().send()


	@staticmethod
	def get_timestamp(t):
		if isinstance(t, str):
			return int(time.mktime(time.strptime(t, self.DATE_FORMAT)))
		else:
			raise ValueError("Неизвестный формат времени")


class EGTS(Retranslator):
	def __init__(self, ip, port):
		"""EGTS протокол
		https://www.swe-notes.ru/post/protocol-egts/

		ip (str): ip адрес получателся
		port (int): порт получаетеля
		"""

		super().__init__("EGTS", ip, port)
		self.pid = 1
		self.rid = 1


	def add_template(self, action, **data):
		assert action in self.protocol.keys(), 'Неизвестное имя добавляемого шаблона'

		def paste_data(params, data):
			for n in params.keys():
				if isinstance(params[n], str):
					if "*"==params[n][0]:
						try:
							params[n] = data[params[n][1:]]
						except KeyError:
							try:
								params[n] = getattr(self, params[n][1:])
							except KeyError:
								raise KeyError(f"Не хватает параметра '{params[n][1:]}' в пакете")
				else:
					continue

			return params

		packet = bytes()
		for name, params in self.protocol[action].items():
			fmt = self.protocol["FORMATS"][name]
			params = paste_data(params, data)
			block = Retranslator.processing(fmt, params, '=')

			if name=="EGTS_PACKET_HEADER":
				control_sum = crc8(block)
				control_sum = struct.pack("<B", control_sum)
				block += control_sum

			packet += block
			self.make_log("info", f"[{action}] Добавлен блок '{name}' (size {len(block)} bytes")

		control_sum = crc16(packet, 11, len(packet)-11)
		control_sum = struct.pack("<H", control_sum)
		self.make_log("info", f"[{action}] Добавлена контрольная сумма записи (size {len(control_sum)} bytes)")
		self.packet += packet + control_sum

		self.inc_pid()
		self.inc_rid()
		

	def inc_pid(self):
		if self.pid == 0xffff:
			self.pid = 0

		else:
			self.pid += 1

		return self.pid


	def inc_rid(self):
		if self.rid == 0xffff:
			self.rid = 0

		else:
			self.rid += 1

		return self.rid


	@staticmethod
	def get_time():
		egts_time = 1262289600 #timestamp 2010-01-01 00:00:00
		return int(time.time())-egts_time


	@staticmethod
	def get_lat(value):
		return int((value/90) * 0xFFFFFFFF)


	@staticmethod
	def get_lon(value):
		return int((value/180) * 0xFFFFFFFF)
