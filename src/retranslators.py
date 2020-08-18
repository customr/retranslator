import struct
import time

from datetime import datetime
from binascii import hexlify, a2b_hex

from src.logs.log_config import logger
from src.core import Retranslator, TCPConnections
from src.crc import crc8, crc16, crc16_arc, crc16_modbus
from src.utils import *

class Wialon(Retranslator):

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
		super().__init__("Wialon")
		
	
	def send(self, ip, port, row, settings):
		self.data = {}
		self.settings = settings

		if not isinstance(row['datetime'], int):
			row['datetime'] = int(Retranslator.utc_to_local(row['datetime']).timestamp())
		packet = self.pack_record(**row)
		response = TCPConnections.send(ip, port, packet)
		if response==b'11' or response==b'f0' or response==b'':
			return 1, response
		
		return 0, response


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
				endiannes = '>'
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
		
		if data.get('t_ret', None):
			self.add_template('temp', temp=data["t_ret"])
		
		elif data.get('temp', None):
			self.add_template('temp', temp=data["temp"])

		packet_size = len(self.packet)
		packet_size = struct.pack("<I", packet_size)
		self.packet = packet_size + self.packet

		return self.packet


class EGTS(Retranslator):
	def __init__(self):
		"""EGTS протокол
		https://www.swe-notes.ru/post/protocol-egts/
		"""

		super().__init__("Egts")
		self.data = {"pid":0, "rid":0}
		self.auth_imei = {}


	def send(self, ip, port, row, settings):
		self.settings = settings

		if isinstance(row['datetime'], datetime):
			row['datetime'] = int(row['datetime'].timestamp())
			
		if row['imei']!=self.auth_imei.get(f"{ip}:{port}"):
			self.auth_imei[f"{ip}:{port}"] = ''
			self.packet = bytes()
			self.add_template("authentication", imei=str(row['imei']))
			if TCPConnections.send(ip, port, self.packet)==-1:
				return 0, 0
			
			self.auth_imei[f"{ip}:{port}"] = str(row['imei'])
			
		rec_packet = self.pack_record(**row)
		response = TCPConnections.send(ip, port, rec_packet)
		if response==-1:
			self.auth_imei[f"{ip}:{port}"]
			return 0, 0

		if response[-6:-4]==b'00':
			return 1, response[-6:-4]
			
		elif response[-6:-4]==b'99' or response[-6:-4]==b'97':
			logger.info("EGTS Необходима повторная авторизация\n")
			self.auth_imei[f"{ip}:{port}"] = ''
			TCPConnections.close_conn(ip, port)
			if TCPConnections.connect(ip, int(port))==-1:
				self.auth_imei[f"{ip}:{port}"] = ''
				TCPConnections.NOT_CONNECTED.append((ip, port))
				return 0, 0 
				
			self.data = {"pid":0, "rid":0}
			self.packet = bytes()
			self.add_template("authentication", imei=str(row['imei']))
			response = TCPConnections.send(ip, port, self.packet)
			if response==-1:
				self.auth_imei[f"{ip}:{port}"] = ''
				return 0, response
			
			elif response[-6:-4]==b'00':
				self.auth_imei[f"{ip}:{port}"] = str(row['imei'])
				
			else:
				return 0, response[-6:-4]
				
			response = TCPConnections.send(ip, port, rec_packet)
			if response[-6:-4]==b'00':
				return 1, response[-6:-4]
			
			else:
				return 0, response[-6:-4]
				
		else:
			TCPConnections.close_conn(ip, port)
			TCPConnections.NOT_CONNECTED.append((ip, port))
			return 0, response[-6:-4]


	def add_template(self, action, **data):
		assert action in self.protocol.keys(), 'Неизвестное имя добавляемого шаблона'
		
		if data.get('datetime', None):	
			data['datetime'] = self.get_egts_time(data['datetime'])
		
		else:
			data.update({"datetime": self.get_egts_time(time.time())})
			
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
			logger.debug(f"EGTS [{action}] Добавлен блок '{name}' (size {len(block)} bytes)\n{hexlify(block)}")

		control_sum = crc16(packet, 11, len(packet)-11)
		control_sum = struct.pack("<H", control_sum)
		logger.debug(f"EGTS [{action}] Добавлена контрольная сумма записи (size {len(control_sum)} bytes)\n{hexlify(control_sum)}")
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
		posflags = 9 #флаг VLD
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


class WialonIPS(Retranslator):
	def __init__(self):
		"""WialonIPS протокол
		http://extapi.wialon.com/hw/cfg/Wialon%20IPS_v_2_0.pdf
		"""
		super().__init__("WialonIPS")
		self.auth_imei = {}


	def send(self, ip, port, row, settings):
		self.data = {}
		self.settings = settings

		if isinstance(row['datetime'], datetime):
			row['datetime'] = int(row['datetime'].timestamp())
			
		if str(row['imei'])!=self.auth_imei.get(f"{ip}:{port}"):
			if self.auth_imei.get(f"{ip}:{port}", None):
				logger.info("WialonIPSRetranslator Необходима повторная авторизация\n")
				self.auth_imei[f"{ip}:{port}"] = ''
				TCPConnections.close_conn(ip, port)
				if TCPConnections.connect(ip, int(port))==-1:
					TCPConnections.NOT_CONNECTED.append((ip, port))
					return 0, 0
				
			packet = self.add_block('authentication', imei=str(row['imei']))
			response = TCPConnections.send(ip, port, packet)
			if response==-1:
				return 0, 0

			else:
				response = str(a2b_hex(response))
				try:
					response = response.split('#')[2][:-1].rstrip('\\r\\n')
				except Exception as e:
					logger.error(f"Не удалось дешифровать ответ сервера\n")
					return 0, response
					
				if response!='1':
					logger.debug(f"Сервер вызвал ошибку")
					return 0, response
				
				else:
					self.auth_imei[f"{ip}:{port}"] = str(row['imei'])
		
		packet = self.add_block('posinfo', **row)
		response = TCPConnections.send(ip, port, packet)

		if response==-1:
			self.auth_imei[f"{ip}:{port}"] = ''
			return 0, 0

		else:
			response = str(a2b_hex(response))
			try:
				response = response.split('#')[2][:-1].rstrip('\\r\\n')
			except Exception as e:
				logger.error(f"Не удалось дешифровать ответ сервера {response}\n")
				return 0, response
				
			if response!='1':
				logger.debug(f"Сервер вызвал ошибку {response}")
				return 0, response
			else:
				return 1, response


	def add_block(self, action, **data):
		"""
		Добавляет часть пакета описанную в json
		name (str): имя блока
		data (kwargs): параметры в соотвествии с выбранным блоком 
		"""
		last =  ';'
		if action=='posinfo':
			data['speed'] = int(data['speed'])
			data['direction'] = int(data['direction'])
			data = self.handle_data(data)
			last = '|'
			
		symbol, params = self.protocol[action]
		params = self.paste_data_into_params(params, data)

		header = f"#{symbol}#"
		block = ';'.join(list(map(str, params.values())))+last
		crc = str(hex(crc16_arc(bytes(block.encode('ascii')))))[2:].upper()
		block = header + block + crc + '\r\n'
		block = bytes(block.encode('ascii'))

		logger.debug(f"WialonIPS Добавлен блок [{action}] (size {len(block)} bytes)\n{hexlify(block)}")
		logger.debug(f"{params.values()}")
		return block
		

	@staticmethod
	def handle_data(data):
		dt = datetime.fromtimestamp(data['datetime'])
		date = dt.strftime("%d%m%y")
		time = dt.strftime("%H%M%S")

		params = f"ign:1:{data['ignition']}"
		params += f",sens:1:{data['sensor']}"
		
		if data.get('t_ret', None):
			params += f",temp:2:{data['t_ret']}"
		
		elif data.get('temp', None):
			params += f",temp:2:{data['temp']}"

		lon1 = int(data['lat'])
		lon1 = lon1*100 + (data['lat']%1)*60
		lon1 = '0'*(4-len(str(int(lon1)))) + str(lon1)

		lat1 = int(data['lon'])
		lat1 = lat1*100 + (data['lon']%1)*60
		lat1 = '0'*(5-len(str(int(lat1)))) + str(lat1)

		lon2 = 'N'
		lat2 = 'E'

		data.update({
			"date": date, 
			"time": time,
			"lon1": lon1,
			"lon2": lon2,
			"lat1": lat1,
			"lat2": lat2,
			"params":params
			})

		return data


class GalileoSky(Retranslator):
	def __init__(self):
		"""Galileosky протокол (эмуляция трекера)
		https://7gis.ru/assets/files/docs/manuals_ru/opisanie-protokola-obmena-s-serverom.pdf
		"""
		super().__init__('GalileoSky')
		self.auth_imei = {}
	

	def send(self, ip, port, row, settings):
		self.data = {}
		self.settings = settings

		if isinstance(row['datetime'], datetime):
			row['datetime'] = int(Retranslator.utc_to_local(row['datetime']).timestamp())
		
		if str(row['imei'])!=self.auth_imei.get(f"{ip}:{port}"):
			if self.auth_imei.get(f"{ip}:{port}", None):
				logger.info("GalileoSkyTrackerEmu Необходима повторная авторизация\n")
				self.auth_imei[f"{ip}:{port}"] = ''
				TCPConnections.close_conn(ip, port)
				if TCPConnections.connect(ip, int(port))==-1:
					TCPConnections.NOT_CONNECTED.append((ip, port))
					return 0, 'Connection error'
				
			packet = self.add_block('authentication', imei=str(row['imei']))
			response = TCPConnections.send(ip, port, packet)
			self.auth_imei[f"{ip}:{port}"] = str(row['imei'])
		
		packet = b''
		packet = self.add_block('posinfo', **row)
		response = TCPConnections.send(ip, port, packet)
		return 1, response
		
	
	def add_block(self, action, **data):
		"""
		Добавляет часть пакета описанную в json
		name (str): имя блока
		data (kwargs): параметры в соотвествии с выбранным блоком 
		"""
		
		if action=='posinfo':
			if self.settings['sensor_inversion']:
				inversion = 1
			else:
				inversion = 0

			data['sat_num'] = 0b1111 if data['sat_num']>0b1111 else data['sat_num']
			data['lat'] *= 1000000
			data['lon'] *= 1000000
			data['lat'] = int(data['lat'])
			data['lon'] = int(data['lon'])
			data['speed'] *= 10
			data['speed'] = int(data['speed'])
			data['ignsens'] = ((data['sensor'] ^ inversion)<<2)+data['ignition']
			if not data.get('temp'):
				if not data.get('temp2'):
					data['temp'] = 0
				else:
					data['temp'] = data['temp2']
 
			data['temp'] = int(data['temp'])
			if data['temp']>120:
				data['temp'] = 0
 
			if not data.get('voltage'):
				data['voltage'] = 0
			else:
				data['voltage'] = int(data['voltage']*1000)

		fmt = self.protocol['FORMATS'][action]
		params = self.protocol['ACTIONS'][action]
		params = self.paste_data_into_params(params, data)
		block = Retranslator.processing(fmt, params, '<')
		block = struct.pack('<B', 1) + struct.pack('<H', len(block)) + block
		block += struct.pack('<H', crc16_modbus(block))

		logger.debug(f"GalileoSkyTrackerEmu Добавлен блок [{action}] (size {len(block)} bytes)\n{hexlify(block)}")
		logger.debug(f"{params.values()}")
		return block