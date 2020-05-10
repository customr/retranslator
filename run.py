import binascii
import time

from src.retranslators import WialonRetranslator, EGTS
from src.core import TCPConnection

#пишите здесь сервера для ретрансляции в формате retranslatorname(host, port)
RETRANSLATORS = [
	#WialonRetranslator('10664.flespi.gw', 29373),
	EGTS('77.123.137.98', 20629)
]

records = [
{"imei": 352094088980534,	"time": '2020-05-08 20:46:15', "lat":53.478275,	"lon":50.147670,	"speed": 124,	"direction": 149,	"sens": 0, 	"ign": 1, "sat_num": 15},
{"imei": 352094088980534,	"time": '2020-05-08 20:46:13', "lat":53.468577,	"lon":50.181085,	"speed": 125,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-08 20:46:10', "lat":53.471673,	"lon":50.169868,	"speed": 131,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 16},	
{"imei": 352094088980534,	"time": '2020-05-08 20:44:38', "lat":53.462458,	"lon":50.203242,	"speed": 121,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-08 20:45:19', "lat":53.465540,	"lon":50.192127,	"speed": 112,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},
{"imei": 352094088980534,	"time": '2020-05-08 20:45:20', "lat":53.456285,	"lon":50.225587,	"speed": 124,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 14},	
{"imei": 352094088980534,	"time": '2020-05-08 20:44:29', "lat":53.459363,	"lon":50.214400,	"speed": 124,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-08 20:43:26', "lat":53.450128,	"lon":50.247798,	"speed": 103,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 13},	
{"imei": 352094088980534,	"time": '2020-05-08 20:43:52', "lat":53.453200,	"lon":50.236598,	"speed": 116,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},
{"imei": 352094088980534,	"time": '2020-05-08 20:42:35', "lat":53.443348,	"lon":50.269665,	"speed": 103,	"direction": 151,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-08 20:43:00', "lat":53.447015,	"lon":50.259010,	"speed": 94,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 16},	
{"imei": 352094088980534,	"time": '2020-05-08 20:41:57', "lat":53.435323,	"lon":50.290155,	"speed": 105,	"direction": 151,	"sens": 0, 	"ign": 1, "sat_num": 14},	
{"imei": 352094088980534,	"time": '2020-05-08 20:42:07', "lat":53.439365,	"lon":50.279785,	"speed": 104,	"direction": 152,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-08 20:41:48', "lat":53.427238,	"lon":50.310703,	"speed": 103,	"direction": 151,	"sens": 0, 	"ign": 1, "sat_num": 12},	
]

# for record in records:
# 	posinfo = {key:record[key] for key in ['lon', 'lat', 'speed', 'direction', 'sat_num']}
# 	RETRANSLATORS[0].add_x("posinfo", **posinfo)
# 	RETRANSLATORS[0].add_x("ign", ign=record["ign"])
# 	RETRANSLATORS[0].add_x('sens', sens=record["sens"])
# 	RETRANSLATORS[0].fill_header(record["imei"], int(time.time()))
# 	RETRANSLATORS[0].send()
# 	time.sleep(2)

RETRANSLATORS[0].add_template("authentication", imei=b'352094088980534')
RETRANSLATORS[0].send()
print(RETRANSLATORS[0].server_answer)
