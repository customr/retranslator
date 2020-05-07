import struct

from src.core import Retranslator


class WialonRetranslator(Retranslator):

	SUCCESS_CODE = 17 #код, который принимает рентранслятор при успешном принятии пакета
	FLAGS = 3 #битовая маска пакета (местоположение, цифр. входы)
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


	def add_x(self, name, **params):
		self.check_header()
		super().add_x(name, **params)
		if not self.protocol["special_blocks"].get(name, None):
			datatype = self.DATATYPES[self.protocol["blockheader_data"][name]['data_type']]
			self.packet_format += datatype
		

	def send(self):
		if self.server_answer: 
			self.server_answer = None

		super().send()
		answer = int.from_bytes(self.server_answer, byteorder="big")
		if answer==self.SUCCESS_CODE:
			self.make_log("info", f"Сервер принял пакет (code={self.SUCCESS_CODE})")
			return 0

		else:
			self.make_log("error", f"Сервер отверг пакет (code={answer})")
			return -1


	def check_header(self):
		if not self.header_is_ready:
			self.packet_format += ''.join(self.protocol['header'].values())
			self.packet_params.extend([None, None, None, None])
			self.make_log("info", "Добавлен пустой блок 'header' в пакет данных")
			self.header_is_ready = True


	def fill_header(self, imei, time):
		assert self.header_is_ready, "Header еще не был создан!"

		self.packet_format, self.packet_params = Retranslator.handler(self.packet_format, self.packet_params)
		
		#исключение в протоколе: little endian
		self.packet_params = self.packet_params[1:]
		self.packet_format = self.packet_format[1:]
		packet_size = struct.calcsize(self.packet_format)-14
		packet_size = struct.pack("<i", packet_size)

		self.packet += packet_size 

		self.packet_params[0] = imei
		self.packet_params[1] = time
		self.packet_params[2] = self.FLAGS

		self.make_log("info", "В 'header' внесены данные")


class EGTS(Retranslator):
	def __init__(self, ip, port):
		"""EGTS протокол
		https://www.swe-notes.ru/post/protocol-egts/

		ip (str): ip адрес получателся
		port (int): порт получаетеля
		"""

		super().__init__("EGTS", ip, port)


	def authorization(self):
		pass
