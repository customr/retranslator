from db_worker import *

imei = input("Введите imei: ")
frm = input("Введите дату и время с какого момента начать выкладывать в формате YYYY-MM-DD HH:MM:SS : ")
to = input("Введите дату и время до какого момента начать выкладывать в формате YYYY-MM-DD HH:MM:SS : ")

with closing(pymysql.connect(**CONN)) as connection:
	records = get_rec_from_to(connection, params['imei'], params['from'], params['to'])
	print(f"Найдено {len(records)} записей для {imei} в промежутке {frm} - {to}")
	for row in records:
		send_row(connection, row)