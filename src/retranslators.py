import struct

from src.core import Retranslator


class WialonRetranslator(Retranslator):

	FLAGS = 0x00000003 #битовая маска пакета (местоположение, цифр. входы)
	DATATYPES = { #типы блока данных описанных в документации
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
		if not self.protocol["blocks_format"].get(name, None):
			self.packet_format += self.DATATYPES[self.protocol["blockheader_data"][name]['data_type']]
		

	def check_header(self):
		if not self.header_is_ready:
			self.packet_format += ''.join(self.protocol['header'].values())
			self.packet_params.extend([None, None, None, None])
			self.make_log("info", "Добавлен пустой блок 'header' в пакет данных")
			self.header_is_ready = True


	def fill_header(self, imei, time):
		assert self.header_is_ready, "Header еще не был создан!"

		#исключение в протоколе: little endian
		packet_size = struct.calcsize(self.packet_format[1:])
		packet_size = struct.pack("<i", packet_size)
		self.packet_params = self.packet_params[1:]
		self.packet_format = self.packet_format[1:]
		self.packet += packet_size 

		self.packet_params[0] = imei
		self.packet_params[1] = time
		self.packet_params[2] = self.FLAGS

		self.make_log("info", "В 'header' внесены данные")