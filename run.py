from db_worker import *


def send_records(records):
	for record in records:
		for protocol in RETRANSLATORS.keys():
			for connection in CONNECTIONS[protocol]:
				result_code = RETRANSLATORS[protocol].send_record(connection, **record)
				if result_code:
					with open(ID_PATH, 'w') as idp:
						idp.write(str(record['id']))
						raise RuntimeError('Непредвиденная остановка ретрансляции')

	with open(ID_PATH, 'w') as idp:
		idp.write(str(records[-1]['id']))


if __name__=="__main__":
	with closing(pymysql.connect(**CONN)) as connection:
		CONNECTIONS = get_connections(connection)

		while True:
			RECORDS = get_records(connection)
			send_records(RECORDS)