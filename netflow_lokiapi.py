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
            data.append(netflowObj(src_ip=newflow['source']['ip'], des_ip=newflow['destination']['ip'], packets=newflow['network']['packets'],
                    bytes=newflow['network']['bytes'], proto=newflow['network']['transport']))
        except:
            print("parse_netflow_obj_error", data)
    return data
