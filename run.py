from src.retranslators import WialonRetranslator


#пишите здесь сервера для ретрансляции в формате retranslatorname(host, port)
RETRANSLATORS = [
	WialonRetranslator('localhost', 8080),
]

SAMPLE = {
	"posinfo":{
		"lat": 		49.1903648,
		"lon": 		55.7305664,
		"alt": 		106.0,
		"speed": 	54,
		"direction":326,
		"nsat": 	11
	},
	"ign": 1,
	"sens": 0
}


RETRANSLATORS[0].add_x("posinfo", **SAMPLE["posinfo"])
RETRANSLATORS[0].add_x("ign", ign=SAMPLE["ign"])
RETRANSLATORS[0].add_x('sens', sens=SAMPLE["sens"])
RETRANSLATORS[0].fill_header("353976013445485", 1565613499)
RETRANSLATORS[0].send()