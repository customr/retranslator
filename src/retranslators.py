import struct
import time

from binascii import hexlify, a2b_hex

from src.logs.log_config import logger
from src.core import Retranslator, TCPConnections
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

	def __init__(self):
		"""WialonRetranslator протокол
		https://gurtam.com/hw/files/Wialon%20Retranslator_v1.pdf
		"""
		super().__init__("WialonRetranslator")
		
	
	def send(self, ip, port, row):
		packet = self.pack_record(**row)
		response = TCPConnections.send(ip, port, packet)
		if response==b'11':
			return 1
		
		return 0


	def add_template(self, name, **params):
		"""
		Добавляет часть пакета описанную в json

		name (str): имя блока (в blocks_format и blockheader_data)
		params (kwargs): параметры в соотвествии с выбранным блоком 
		"""
		block = bytes()

		if name!='header':
			fmt = self.protocol["FORMATS"]['blockheader']
			data = self.protocol["PARAMS"]["blockheader"][name]
			block += Retranslator.processing(fmt, data, '>')
		
		fmt = self.protocol["FORMATS"].get(name, '')
		if fmt:
			block += Retranslator.processing(fmt, params, '>')

		else:
			datatype = data.get('data_type', None)
			if datatype:
				datatype = self.DATATYPES[datatype]
				endiannes = ">"
				if datatype=='d': endiannes='<'
				block += Retranslator.pack_data(datatype, params.values(), endiannes)

		self.packet += block
		logger.debug(f"Добавлен блок '{name}' (size {len(block)} bytes)\n{hexlify(block)}")


	def pack_record(self, **data):
		self.packet = bytes()
		pdata = {key:data[key] for key in ['lon', 'lat', 'speed', 'direction', 'sat_num']}
		self.add_template("header", imei=str(data["imei"]), tm=data['datetime'])
		self.add_template("posinfo", **pdata)
		self.add_template("ign", ign=data["ignition"])
		self.add_template('sens', sens=data["sensor"])
		self.add_template('temp', temp=data["temp"])

		packet_size = len(self.packet)
		packet_size = struct.pack("<I", packet_size)
		self.packet = packet_size + self.packet

		return self.packet


class EGTS(Retranslator):
	def __init__(self):
		"""EGTS протокол
		https://www.swe-notes.ru/post/protocol-egts/

		ip (str): ip адрес получателся
		port (int): порт получаетеля
		"""

		super().__init__("EgtsRetranslator")
		self.data = {"pid":0, "rid":0}
		self.auth_imei = ''


	def send(self, ip, port, row):
		if row['imei']!=self.auth_imei:
			self.packet = bytes()
			self.add_template("authentication", imei=str(row['imei']))
			if TCPConnections.send(ip, port, self.packet)==-1:
				return 0
			
			self.auth_imei = str(row['imei'])
			
		rec_packet = self.pack_record(**row)
		response = TCPConnections.send(ip, port, rec_packet)
		if response==-1:
			return 0

		if response[-6:-4]==b'00':
			return 1
			
		elif response[-6:-4]==b'99':
			TCPConnections.close_conn(ip, port)
			TCPConnections.connect(ip, port)
				
			self.packet = bytes()
			self.add_template("authentication", imei=str(row['imei']))
			if TCPConnections.send(ip, port, self.packet)==-1:
				return 0
			
			self.auth_imei = str(row['imei'])
			response = TCPConnections.send(ip, port, rec_packet)
			if response[-6:-4]==b'00':
				return 1
			
			else:
				return 0
		else:
			return 0


	def add_template(self, action, **data):
		assert action in self.protocol.keys(), 'Неизвестное имя добавляемого шаблона'
		
		if data.get('datetime', None):	
			data['datetime'] = self.get_egts_time(data['datetime'])
			
		if action=='posinfo':
			self.handle_spd_and_dir(data['speed'], data['direction'])
			self.handle_posflags(data['ignition'])

		packet = bytes()
		self.data.update(data)
		for name, params in self.protocol[action].items():
			if '$' in name: name = name[:name.index('$')]
			fmt = self.protocol["FORMATS"][name]
			params = self.paste_data_into_params(params, self.data, fmt)
			block = Retranslator.processing(fmt, params, '<')

			if name=="EGTS_PACKET_HEADER":
				control_sum = crc8(block)
				control_sum = struct.pack("<B", control_sum)
				block += control_sum
				self.data["pid"] = self.inc_id(self.data['pid'])

			if name=="EGTS_RECORD_HEADER":
				self.data["rid"] = self.inc_id(self.data['rid'])

			packet += block
			logger.debug(f"[{action}] Добавлен блок '{name}' (size {len(block)} bytes)\n{hexlify(block)}")

		control_sum = crc16(packet, 11, len(packet)-11)
		control_sum = struct.pack("<H", control_sum)
		logger.debug(f"[{action}] Добавлена контрольная сумма записи (size {len(control_sum)} bytes)\n{hexlify(control_sum)}")
		self.packet += packet + control_sum


	def pack_record(self, **data):
		self.packet = bytes()

		rdata = {key:data[key] for key in ['lon', 'lat', 'speed', 'direction', 'sat_num', 'sensor', 'ignition', 'datetime']}
		rdata['lat'] = self.get_lat(rdata['lat'])
		rdata['lon'] = self.get_lon(rdata['lon'])
		rdata.update({"din": rdata['sensor']})
		self.add_template("posinfo", **rdata)
		
		return self.packet 


	def handle_spd_and_dir(self, speed, dr):
		speed *= 10
		dr = 0
		self.data.update({"spd": speed, "dir": dr})


	def handle_posflags(self, ign):
		posflags = 1 #флаг VLD
		if ign:
			posflags += 0b10000 #флаг MV

		self.data.update({"posflags": posflags})


	@staticmethod
	def inc_id(value):
		if value == 0xffff:
			value = 0

		else:
			value += 1

		return value


	@staticmethod
	def get_egts_time(tm):
		egts_time = time.strptime("2010-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
		egts_time = time.mktime(egts_time)

		if isinstance(tm, str):
			tm = Retranslator.get_timestamp(tm)

		return int(tm-egts_time)


	@staticmethod
	def get_lat(value):
		return int((value/90) * 0xFFFFFFFF)


	@staticmethod
	def get_lon(value):
		return int((value/180) * 0xFFFFFFFF)
