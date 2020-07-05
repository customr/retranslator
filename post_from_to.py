from db_worker import *


with closing(pymysql.connect(**CONN)) as connection:
	ipports = get_ipports(connection)
	TCPConnections.connect_many(ipports)
	time.sleep(1)
	imei = input("Введите imei: ")
	print("Дата и время как в базе данных (по Гринвичу)")
	frm = input("Начиная с  : ")
	to = input("Заканчивая : ")
	records = get_rec_from_to(connection, imei, frm, to)
	print(f"Найдено {len(records)} записей")
	for n, row in enumerate(records):
		for ret in RETRANSLATORS.values():
			send_row(connection, row, ret, False)

	
input("Нажмите любую кнопку чтобы выйти...")