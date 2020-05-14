import struct
import time

from binascii import hexlify, a2b_hex

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


	def add_template(self, name, **params):
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
		self.make_log("debug", f"Добавлен блок '{name}' в пакет данных")


	def send(self):
		assert len(self.packet)>0

		packet_size = len(self.packet)
		packet_size = struct.pack("<I", packet_size)
		self.packet = packet_size + self.packet
		super().send()


class EGTS(Retranslator):
	def __init__(self, ip, port):
		"""EGTS протокол
		https://www.swe-notes.ru/post/protocol-egts/

		ip (str): ip адрес получателся
		port (int): порт получаетеля
		"""

		super().__init__("EGTS", ip, port)
		self.pid = 0
		self.rid = 0


	def add_template(self, action, **data):
		assert action in self.protocol.keys(), 'Неизвестное имя добавляемого шаблона'

		if data.get('lat', ''):
			data['lat'] = self.get_lat(data['lat'])

		if data.get('lon', ''):
			data['lon'] = self.get_lon(data['lon'])

		if data.get('time', ''):
			data['time'] = self.get_egts_time(data['time'])

		if action=='posinfo':
			dout = data['sens']
			dout = (dout<<1)+data['ign']
			data.update({"din": dout})

			self.handle_spd_and_dir(data['speed'], data['direction'])

		packet = bytes()

		for name, params in self.protocol[action].items():
			if '$' in name: name = name[:name.index('$')]
			fmt = self.protocol["FORMATS"][name]
			params = self.paste_data_into_params(params, data, fmt)
			self.data.update(params)
			block = Retranslator.processing(fmt, params, '<')

			if name=="EGTS_PACKET_HEADER":
				control_sum = crc8(block)
				control_sum = struct.pack(">B", control_sum)
				block += control_sum
				self.inc_pid()

			packet += block
			self.make_log("debug", f"[{action}] Добавлен блок '{name}' (size {len(block)} bytes)\n{hexlify(block)}")

		control_sum = crc16(packet, 11, len(packet)-11)
		control_sum = struct.pack(">H", control_sum)
		self.make_log("debug", f"[{action}] Добавлена контрольная сумма записи (size {len(control_sum)} bytes)\n{hexlify(control_sum)}")
		self.packet += packet + control_sum

		self.inc_rid()
		print(self.pid, self.rid)


	def handle_spd_and_dir(self, speed, dr):
		spdl = speed % 16
		dirl = dr % 16

		dirh = dr // 16
		spdh = speed // 16

		spdh_alt_dirh = spdl * 16
		spdh_alt_dirh = (spdh_alt_dirh+dirh) * 16 * 16
		spdh_alt_dirh += spdh
		
		self.data.update({"spdl": spdl, "spdh_alt_dirh": spdh_alt_dirh, "dirl": dirl})


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
	def get_egts_time(tm):
		egts_time = 1262289600 #timestamp 2010-01-01 00:00:00

		if isinstance(tm, str):
			tm = Retranslator.get_timestamp(tm)

		return tm-egts_time


	@staticmethod
	def get_lat(value):
		return int((value/90) * 0xFFFFFFFF)


	@staticmethod
	def get_lon(value):
		return int((value/180) * 0xFFFFFFFF)
