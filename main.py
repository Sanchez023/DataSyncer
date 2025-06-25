import mysql.connector 

class Connector:
    def __init__(self,host:str,port:int,user:str,passwd:str):
        self.isAlive = False
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
    
    def close(self,)->None:
        return None
    
    # def query(self,):
    #     return 0
    
    def testConnect(self,)->bool:
        return False

class MysqlConnector(Connector):

    # 初始化mysql连接
    def __init__(self,host:str,port:int,user:str,passwd:str):
        super().__init__(host,port,user,passwd)
        self.connector = mysql.connector.connect(host=host,port=port,user=user,passwd=passwd)
        print(f"初始化MYSQL连接: \n 地址：{host} \n 端口：{port} \n 用户：{user} \n 密码：{passwd}")
        
    
    # 测试连接并返回连接状态
    def testConnect(self) -> bool:
        if self.connector.is_connected():
            self.isAlive = True
        else:
            self.isAlive = False
        return self.isAlive
    
    # 发送查询语句
    def query(self,tableName:str,columns_list:list[str],condition:str="1=1"):
        SQL_STATEMENT = "SELECT %s FROM %s WHERE %s;"
        columns = ",".join([column.join(["`","`"]) for column in columns_list])
        val = (columns,tableName,condition)
        with self.connector.cursor(prepared=True,dictionary=True) as cursor:
            cursor.execute(SQL_STATEMENT,val)
            res = cursor.fetchmany(size=10)
            for row in res:
                print(row) 
    
    def close(self)->None:
        return self.connector.close()
    
if __name__ == "__main__":
    mysqlCon = MysqlConnector("localhost",3306,"leo","leo130")
    print(mysqlCon.testConnect())
    mysqlCon.close()
