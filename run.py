import binascii

from src.retranslators import WialonRetranslator


#пишите здесь сервера для ретрансляции в формате retranslatorname(host, port)
RETRANSLATORS = [
	WialonRetranslator('10664.flespi.gw', 29373),
]

SAMPLE = {
	"imei": "352094088980534",
	"time": 1588435417,
	"posinfo":{
		"lat": 		53.545237,
		"lon": 		49.348093,
		"alt": 		0,
		"speed": 	0,
		"direction":15,
		"nsat": 	15
	},
	"ign": 1,
	"sens": 0
}


RETRANSLATORS[0].add_x("posinfo", **SAMPLE["posinfo"])
RETRANSLATORS[0].add_x("ign", ign=SAMPLE["ign"])
RETRANSLATORS[0].add_x('sens', sens=SAMPLE["sens"])
RETRANSLATORS[0].fill_header(SAMPLE["imei"], SAMPLE["time"])
RETRANSLATORS[0].send()