from src.retranslators import GalileoSkyTrackerEmu
import binascii
import datetime
t = GalileoSkyTrackerEmu()
b = t.add_block('posinfo', imei='123456789012345', datetime=datetime.datetime(2020,6,20,14,52,12,52), lon=53.12, lat=49.24, speed=23.1, sat_num=14, ignition=1, sensor=0)
print(binascii.hexlify(b))
# import requests
# import json
# cookies = {}
# headers = {}
# params = (
#     ('path', '/main/property/list^'),
# )
# data = '{"requiresCounts":true,"skip":0,"take":50000}'
# response = requests.post('http://185.247.140.13/data', headers=headers, params=params, cookies=cookies, data=data, verify=False)
# x = json.loads(response.text)
# while True:
#     print(eval(input()))