import pymysql

class MySQL:
    def __init__(self):
        self.connc = pymysql.Connect(
                    host='127.0.0.1',
                    user='root',
                    password="123456",
                    database='ska',
                    port=3306,
                    charset="utf8",
        )
    
    def execute(self, sql:str):
        cursor = self.connc.cursor()
        try:
            cursor.execute(sql)
            self.connc.commit()
        except Exception as e:
            self.connc.rollback()
            print(e)
    
        
    def execute_many(self, sql:str, data:list):
        cursor = self.connc.cursor()
        try:
            cursor.executemany(sql, data)
            self.connc.commit()
        except Exception as e:
            self.connc.rollback()
            print(e)

