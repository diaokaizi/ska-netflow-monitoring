from mysql import MySQL
import datetime
import requests
import json
import datetime
from urllib import parse


class netflowObj:
    def __init__(self, src_ip, des_ip, packets, bytes, proto):
        self.src_ip = src_ip
        self.des_ip = des_ip
        self.packets = packets
        self.bytes = bytes
        self.proto = proto

    def __str__(self) -> str:
        return f"{self.src_ip} -> {self.des_ip} {self.packets}======{self.bytes}"

    def get_key(self) -> str:
        return f"{self.src_ip}:{self.des_ip}:{self.bytes}"

def lokiapi(host: str, start: datetime.datetime, end: datetime.datetime, limit: int = 1000, search: str = "") -> list[netflowObj]:
    loki_query = '{job = "netflow"}'
    if search:
        loki_query = f'{loki_query} |= `{search}`'
    params = {
        'query': loki_query,
        'start': start.timestamp(),
        'end': end.timestamp(),
        'limit': limit
    }
    query = parse.urlencode(query=params)
    base_url = f"http://{host}/loki/api/v1/query_range"
    resp = requests.get(f'{base_url}?{query}')
    # url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}&limit={limit}&start={startdt.timestamp()}&end={enddt.timestamp()}"
    if resp.status_code != 200:
        raise Exception(f"lokiapi_request_failed, {resp.text}")
    return parse_lokiapi_data(resp.text)

def parse_lokiapi_data(data : str) -> list[netflowObj] :
    f1, f2 = 0, 0
    try:
        result = json.loads(data)['data']['result']
        if len(result) == 0: #数据为空
            return []
        source = result[0]['values']
        newflows = [json.loads(record[1]) for record in source]
    except:
        raise Exception("parse_lokiapi_data_error") 
    data = []
    for newflow in newflows:
        try:
            # 添加过滤条件egressInterface==35
            if newflow['flowDirection'] != 1:
                f1 += newflow['network']['bytes']
                continue
            else:
                f2 += newflow['network']['bytes']
            data.append(netflowObj(src_ip=newflow['source']['ip'], des_ip=newflow['destination']['ip'], packets=newflow['network']['packets'],
                    bytes=newflow['network']['bytes'], proto=newflow['network']['transport']))
        except Exception as e:
            print("parse_netflow_obj_error", data, e)
    return data, f1, f2

def get_latest_5m_start_datetime():
    now = datetime.datetime.now()
    timestamp = datetime.datetime.timestamp(now)
    return datetime.datetime.fromtimestamp(timestamp - timestamp % 300 - 300)
host="223.193.36.79:7135" # netflow loki地址
ip = "202.127.3.156"
start = get_latest_5m_start_datetime()
data = []
data, f1, f2 = lokiapi(host=host, start = start, end = start + datetime.timedelta(minutes=5), limit=500000, search=ip)

print(f1, f2)
dic = {}
total1, total2 = 0, 0
for _ in data:
    dic[str(_)] = dic.get(str(_), 0) + 1
    total1 += _.bytes
print(total1)

# print(total1)

# total, p = 0, 0
# for _ in data:
#     total += _.bytes
#     if dic[str(_)] > 1:
#         p += _.bytes

# print(total, p)


	
