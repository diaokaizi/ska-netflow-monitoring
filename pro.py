from mysql import MySQL
from netflow_lokiapi import lokiapi
import datetime
def get_latest_5m_start_datetime():
    now = datetime.datetime.now()
    timestamp = datetime.datetime.timestamp(now)
    return datetime.datetime.fromtimestamp(timestamp - timestamp % 300 - 300)
host="223.193.36.79:7135" # netflow loki地址
ip = "202.127.3.156"
start = get_latest_5m_start_datetime()
data = []
data = lokiapi(host=host, start = start, end = start + datetime.timedelta(minutes=5), limit=50000, search=ip)
dic = {}
total1, total2 = 0, 0
for _ in data:
    dic[str(_)] = dic.get(str(_), 0) + 1
    total1 += _.bytes

d = 0
for k, v in dic.items():
    print(k, v)
    d += 1
    total2 += int(k.split("======")[1])
print(len(data), d)
print(total1, total2)
# print(len(data), d)

# total, p = 0, 0
# for _ in data:
#     total += _.bytes
#     if dic[str(_)] > 1:
#         p += _.bytes

# print(total, p)


	
