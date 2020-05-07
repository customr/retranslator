from time import time

from src.retranslators import WialonRetranslator


#пишите здесь сервера для ретрансляции в формате retranslatorname(host, port)
RETRANSLATORS = [
	WialonRetranslator('10664.flespi.gw', 29373),
]

SAMPLE = {
	"imei": "352094088980534",
	"time": int(time()),
	"posinfo":{
		"lat": 		53.555237,
		"lon": 		49.343093,
		"alt": 		0.0,
		"speed": 	17,
		"direction":15,
		"nsat": 	12
	},
	"ign": 1,
	"sens": 0,
	"temp": 45
}

for retranslator in RETRANSLATORS:
	#retranslator.add_x("avl_inputs", avl_inputs=1)
	retranslator.add_x("posinfo", **SAMPLE["posinfo"])
	retranslator.add_x("ign", ign=SAMPLE["ign"])
	retranslator.add_x('sens', sens=SAMPLE["sens"])
	retranslator.add_x('temp', temp=SAMPLE["temp"])
	retranslator.fill_header(SAMPLE["imei"], SAMPLE["time"])
	retranslator.send()

#66000000333532303934303838393830353334005eb40557000000030bbb000000270102706f73696e666f00066344a2d0ac49401f9dbaf259ae4a400000000000000000001100e0120bbb0000000a000369676e00000000010bbb0000000b000373656e730000000000
