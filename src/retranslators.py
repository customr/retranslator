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
		self.make_log("debug", f"Добавлен блок '{name}' (size {len(block)} bytes)\n{hexlify(block)}")


	def send(self, uid):
		assert len(self.packet)>0

		packet_size = len(self.packet)
		packet_size = struct.pack("<I", packet_size)
		self.packet = packet_size + self.packet
		super().send(uid)


	def send_posinfo(self, **data):
		pdata = {key:data[key] for key in ['lon', 'lat', 'speed', 'direction', 'sat_num', 'time']}
		self.add_template("header", imei=str(data["imei"]), tm=int(time.time()))
		self.add_template("posinfo", **pdata)
		self.add_template("ign", ign=data["ign"])
		self.add_template('sens', sens=data["sens"])
		self.send(data['id'])


class EGTS(Retranslator):
	def __init__(self, ip, port):
		"""EGTS протокол
		https://www.swe-notes.ru/post/protocol-egts/

		ip (str): ip адрес получателся
		port (int): порт получаетеля
		"""

		super().__init__("EGTS", ip, port)
		self.data = {"pid":1, "rid":1}
		self.authorized_imei = ''


	def add_template(self, action, **data):
		assert action in self.protocol.keys(), 'Неизвестное имя добавляемого шаблона'

		if action=='posinfo':
			self.handle_spd_and_dir(data['speed'], data['direction'])

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
			self.make_log("debug", f"[{action}] Добавлен блок '{name}' (size {len(block)} bytes)\n{hexlify(block)}")

		control_sum = crc16(packet, 11, len(packet)-11)
		control_sum = struct.pack("<H", control_sum)
		self.make_log("debug", f"[{action}] Добавлена контрольная сумма записи (size {len(control_sum)} bytes)\n{hexlify(control_sum)}")
		self.packet += packet + control_sum


	def send_posinfo(self, **data):
		if str(self.authorized_imei)!=str(data['imei']):
			self.add_template("authentication", imei=str(data['imei']), time=int(time.time()))
			rcode = self.send()
			if not rcode:
				self.authorized_imei = str(data['imei'])

			else:
				raise RuntimeError('Авторизация не удалась')

		rdata = {key:data[key] for key in ['lon', 'lat', 'speed', 'direction', 'sat_num', 'sens', 'ign', 'time']}
		rdata['lat'] = self.get_lat(rdata['lat'])
		rdata['lon'] = self.get_lon(rdata['lon'])
		rdata['time'] = self.get_egts_time(rdata['time'])
		dout = rdata['sens']
		dout = (dout<<1)+rdata['ign']
		rdata.update({"din": dout})
		self.add_template("posinfo", **rdata)
		self.send(data['id'])


	def handle_spd_and_dir(self, speed, dr):
		#FIX THIS
		spdl = int(speed) % 2**8
		spdh = int(speed) // 2**8
		dirl = dr % 2**8
		dirh = dr // 2**8
		spd_alts_dirh = ((spdl+dirh*2)*2)*2**8+spdh
		self.data.update({"spd_alts_dirh": 0, "dirl": 0})


	@staticmethod
	def inc_id(value):
		if value == 0xffff:
			value = 0

		else:
			value += 1

		return value


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
