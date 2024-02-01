from mysql import MySQL
from netflow_lokiapi import lokiapi
import datetime
def creat_table():
    sql = """CREATE TABLE IF NOT EXISTS netflow (
        id int NOT NULL AUTO_INCREMENT,
        ip char(20) NOT NULL,
        src_bytes int NOT NULL,
        src_packets int NOT NULL,
        des_bytes int NOT NULL,
        des_packets int NOT NULL,
        create_time timestamp NOT NULL,
        primary key (id),
        INDEX  time_index (create_time)
        )ENGINE=InnoDB DEFAULT CHARSET=utf8"""
    MySQL().execute(sql=sql)

def get_latest_5m_start_datetime():
    now = datetime.datetime.now()
    timestamp = datetime.datetime.timestamp(now)
    return datetime.datetime.fromtimestamp(timestamp - timestamp % 300 - 300)


def monitor_5m(host : str, ip : str):
    start = get_latest_5m_start_datetime()
    data = []
    try:
        data = lokiapi(host=host, start = start, end = start + datetime.timedelta(minutes=5), limit=50000, search=ip)
    except Exception as e:
        with open('/root/ska/job_failed.log', 'a+') as f:
            f.write(f"{ip}, {start}, {e} \n")
        print(ip, start, e)
    src_bytes, src_packets, des_bytes, des_packets = 0, 0, 0, 0
    for netflow_obj in data:
        if netflow_obj.src_ip == ip:
            src_bytes += netflow_obj.bytes
            src_packets += netflow_obj.packets
        elif netflow_obj.des_ip == ip:
            des_bytes += netflow_obj.bytes
            des_packets += netflow_obj.packets
    sql_data = (ip, src_bytes, src_packets, des_bytes, des_packets, start)
    sql = """INSERT INTO netflow(
        ip, src_bytes, src_packets, des_bytes, des_packets, create_time)
        VALUES ('%s',%s,%s,%s,%s,'%s') """ % sql_data
    MySQL().execute(sql=sql)

if __name__ == '__main__':
    ip_list = ["202.127.3.156", "202.127.3.157", "202.127.3.158"] # 需要监控的ip
    host="223.193.36.79:7135" # netflow loki地址
    # creat_table()
    for ip in ip_list:
        monitor_5m(host=host, ip=ip)


# SELECT (src_bytes + des_bytes) / 300 as bps, create_time FROM ska.netflow WHERE create_time >= $__timeFrom() AND create_time <= $__timeTo() AND ip = "202.127.3.156"