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
		self.packet_format = ""
		self.packet_params = []
		self.header_is_ready = False


	def add_x(self, name, **params):
		"""
		Добавляет часть пакета описанную в json

		name (str): имя блока (в blocks_format и blockheader_data)
		params (kwargs): параметры в соотвествии с выбранным блоком 
		"""

		self.check_header()

		#добавляем формат описания блока
		blockheader_format = ''.join(self.protocol["blockheader"].values())
		self.packet_format += blockheader_format

		#тут же вставляем эти параметры
		blockheader_data = self.protocol["blockheader_data"][name].values()
		self.packet_params.extend(blockheader_data)

		#добавляем формат для данных
		if self.protocol["special_blocks"].get(name, None):
			data_format = ''.join(self.protocol["special_blocks"][name].values())
			self.packet_format += data_format
			data = self.in_correct_order(self.protocol["special_blocks"].get(name, None), params)
		
		else:
			datatype = self.DATATYPES[self.protocol["blockheader_data"][name]['data_type']]
			self.packet_format += datatype
			data = params.values()

		self.packet_params.extend(data)
		self.make_log("info", f"Добавлен блок '{name}'")
			

	def send(self):
		self.packet_format, self.packet_params = Retranslator.handler(self.packet_format, self.packet_params)
		self.packet += Retranslator.pack_data(self.packet_format, self.packet_params, ">")
		self.make_log("info", "Пакет данных готов к отправке")

		super().send()


	def check_header(self):
		if not self.header_is_ready:
			self.packet_format += ''.join(self.protocol['header'].values())
			self.packet_params.extend([None, None, None, None])
			self.make_log("info", "Добавлен пустой блок 'header' в пакет данных")
			self.header_is_ready = True


	def fill_header(self, imei, tm):
		assert self.header_is_ready, "Header еще не был создан!"

		if not isinstance(imei, str):
			imei = str(imei)

		if not isinstance(tm, int):
			tm = int(time.mktime(time.strptime(tm, self.DATE_FORMAT)))

		self.packet_format, self.packet_params = Retranslator.handler(self.packet_format, self.packet_params)
		
		#исключение в протоколе: little endian
		self.packet_params = self.packet_params[1:]
		self.packet_format = self.packet_format[1:]
		packet_size = struct.calcsize(self.packet_format)-14
		packet_size = struct.pack("<i", packet_size)

		self.packet += packet_size 

		self.packet_params[0] = imei
		self.packet_params[1] = tm
		self.packet_params[2] = self.FLAGS

		self.make_log("info", "В 'header' внесены данные")


	def reset(self):
		self.packet_format = ''
		self.packet_params = []
		self.header_is_ready = False
		super().reset()


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

		def paste(params, data):
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
			params = paste(params, data)
			params = self.in_correct_order(fmt, params)

			fmt = ''.join(fmt.values())

			fmt, params = Retranslator.handler(fmt, params)
			block = Retranslator.pack_data(fmt, params, '=')

			if name=="EGTS_PACKET_HEADER":
				control_sum = crc8(block)
				control_sum = struct.pack("<B", control_sum)
				block += control_sum

			packet += block
			self.make_log("info", f"[{action}] Добавлен блок '{name}' (size {len(block)} bytes")

		control_sum = crc16(packet, 10)
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
