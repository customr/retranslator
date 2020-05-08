import binascii
import time
from src.retranslators import WialonRetranslator
from src.core import TCPConnection

#пишите здесь сервера для ретрансляции в формате retranslatorname(host, port)
RETRANSLATORS = [
	WialonRetranslator('10664.flespi.gw', 29373),
]

records = [
{"imei": 352094088980534,	"time": '2020-05-08 20:46:15', "lon":53.478275,	"lat":50.147670,	"speed": 124,	"direction": 149,	"sens": 0, 	"ign": 1, "sat_num": 15},
{"imei": 352094088980534,	"time": '2020-05-08 20:46:13', "lon":53.468577,	"lat":50.181085,	"speed": 125,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-08 20:46:10', "lon":53.471673,	"lat":50.169868,	"speed": 131,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 16},	
{"imei": 352094088980534,	"time": '2020-05-08 20:44:38', "lon":53.462458,	"lat":50.203242,	"speed": 121,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-08 20:45:19', "lon":53.465540,	"lat":50.192127,	"speed": 112,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},
{"imei": 352094088980534,	"time": '2020-05-08 20:45:20', "lon":53.456285,	"lat":50.225587,	"speed": 124,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 14},	
{"imei": 352094088980534,	"time": '2020-05-08 20:44:29', "lon":53.459363,	"lat":50.214400,	"speed": 124,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-08 20:43:26', "lon":53.450128,	"lat":50.247798,	"speed": 103,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 13},	
{"imei": 352094088980534,	"time": '2020-05-08 20:43:52', "lon":53.453200,	"lat":50.236598,	"speed": 116,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},
{"imei": 352094088980534,	"time": '2020-05-08 20:42:35', "lon":53.443348,	"lat":50.269665,	"speed": 103,	"direction": 151,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-08 20:43:00', "lon":53.447015,	"lat":50.259010,	"speed": 94,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 16},	
{"imei": 352094088980534,	"time": '2020-05-08 20:41:57', "lon":53.435323,	"lat":50.290155,	"speed": 105,	"direction": 151,	"sens": 0, 	"ign": 1, "sat_num": 14},	
{"imei": 352094088980534,	"time": '2020-05-08 20:42:07', "lon":53.439365,	"lat":50.279785,	"speed": 104,	"direction": 152,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-08 20:41:48', "lon":53.427238,	"lat":50.310703,	"speed": 103,	"direction": 151,	"sens": 0, 	"ign": 1, "sat_num": 12},	
]

for retranslator in RETRANSLATORS:
	for record in records:
		posinfo = {key:record[key] for key in ['lat', 'lon', 'speed', 'direction', 'sat_num']}
		retranslator.add_x("posinfo", **posinfo)
		retranslator.add_x("ign", ign=record["ign"])
		retranslator.add_x('sens', sens=record["sens"])
		retranslator.fill_header(record["imei"], record["time"])
		retranslator.send()
		time.sleep(2)

#test = TCPConnection('10664.flespi.gw', 29373)
#test.send(binascii.a2b_hex(b'66000000333532303934303838393830353334005eaad59c000000030bbb000000270102706f73696e666f00066344a2d0ac49401f9dbaf259ae4a400000000000000000001100e0120bbb0000000a000369676e00000000010bbb0000000b000373656e730000000000'))
#test.send(binascii.a2b_hex(b'66000000333532303934303838393830353334005eaad5a5000000030bbb000000270102706f73696e666f002a1900aab8b74a40ace28dcc232549400000000000000000006900970e0bbb0000000a000369676e00000000010bbb0000000b000373656e730000000000'))