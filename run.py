import time

from src.retranslators import WialonRetranslator, EGTS


RETRANSLATORS = [
	#WialonRetranslator('10664.flespi.gw', 29373),
	EGTS('77.123.137.98', 20629)
]

records = [
{"imei": 352094088980534,	"time": '2020-05-14 16:34:15', "lat":53.478275,	"lon":50.147670,	"speed": 124,	"direction": 149,	"sens": 0, 	"ign": 1, "sat_num": 15},
{"imei": 352094088980534,	"time": '2020-05-14 16:34:13', "lat":53.468577,	"lon":50.181085,	"speed": 125,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-14 16:34:10', "lat":53.471673,	"lon":50.169868,	"speed": 131,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 16},	
{"imei": 352094088980534,	"time": '2020-05-14 16:34:38', "lat":53.462458,	"lon":50.203242,	"speed": 121,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-14 16:34:19', "lat":53.465540,	"lon":50.192127,	"speed": 112,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},
{"imei": 352094088980534,	"time": '2020-05-14 16:34:20', "lat":53.456285,	"lon":50.225587,	"speed": 124,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 14},	
{"imei": 352094088980534,	"time": '2020-05-14 16:34:29', "lat":53.459363,	"lon":50.214400,	"speed": 124,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-14 16:34:26', "lat":53.450128,	"lon":50.247798,	"speed": 103,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 13},	
{"imei": 352094088980534,	"time": '2020-05-14 16:34:52', "lat":53.453200,	"lon":50.236598,	"speed": 116,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 15},
{"imei": 352094088980534,	"time": '2020-05-14 16:34:35', "lat":53.443348,	"lon":50.269665,	"speed": 103,	"direction": 151,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-14 16:34:00', "lat":53.447015,	"lon":50.259010,	"speed": 94,	"direction": 147,	"sens": 0, 	"ign": 1, "sat_num": 16},	
{"imei": 352094088980534,	"time": '2020-05-14 16:34:57', "lat":53.435323,	"lon":50.290155,	"speed": 105,	"direction": 151,	"sens": 0, 	"ign": 1, "sat_num": 14},	
{"imei": 352094088980534,	"time": '2020-05-14 16:34:07', "lat":53.439365,	"lon":50.279785,	"speed": 104,	"direction": 152,	"sens": 0, 	"ign": 1, "sat_num": 15},	
{"imei": 352094088980534,	"time": '2020-05-14 16:34:48', "lat":53.427238,	"lon":50.310703,	"speed": 103,	"direction": 151,	"sens": 0, 	"ign": 1, "sat_num": 12},	
]

# for record in records:
# 	posinfo = {key:record[key] for key in ['lon', 'lat', 'speed', 'direction', 'sat_num']}
# 	RETRANSLATORS[0].add_template("header", imei=str(record["imei"]), tm=int(time.time()))
# 	RETRANSLATORS[0].add_template("posinfo", **posinfo)
# 	RETRANSLATORS[0].add_template("ign", ign=record["ign"])
# 	RETRANSLATORS[0].add_template('sens', sens=record["sens"])
# 	RETRANSLATORS[0].send()
# 	time.sleep(2)

RETRANSLATORS[0].add_template("authentication", imei=b'352094088980534', time=int(time.time()))
RETRANSLATORS[0].send()

for record in records:
	data = {key:record[key] for key in ['lon', 'lat', 'speed', 'direction', 'sat_num', 'time']}
	RETRANSLATORS[0].add_template("posinfo", **data)
	RETRANSLATORS[0].send()
	time.sleep(3)

