from mysql.connector.pooling import MySQLConnectionPool
import mysql.connector


class Connector:
    count = 0

    def __init__(self, host: str, port: int, user: str, passwd: str):
        self.isAlive = False
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd

    def close(
        self,
    ) -> None:
        return None

    # def query(self,):
    #     return 0

    def testConnect(
        self,
    ) -> bool:
        return False


class ConnectPool:
    def __init__(self, pool_name: str, size: int, **dbconfig: dict[str, str]):
        self.isAlive = False
        self.poolName = pool_name
        self.size = size
        self.host = dbconfig.get("host")
        self.port = dbconfig.get("port")
        self.user = dbconfig.get("user")
        self.passwd = dbconfig.get("passwd")


class MysqlConnector(Connector):
    '''
    自定义MYSQL连接器
    
    '''

    # 初始化mysql连接
    def __init__(self, host: str, port: int, user: str, passwd: str):
        super().__init__(host, port, user, passwd)
        self.connector = mysql.connector.connect(
            host=host, port=port, user=user, passwd=passwd
        )
        print(
            f"初始化MYSQL连接: \n 地址：{host} \n 端口：{port} \n 用户：{user} \n 密码：{passwd}"
        )

    # 测试连接并返回连接状态
    def testConnect(self) -> bool:

        if self.connector.is_connected():
            self.isAlive = True
        else:
            self.isAlive = False
        print(f"连接状态{self.isAlive}")
        return self.isAlive

    # 发送限定的查询语句
    def query(
        self,
        tableName: str,
        columns_list: list[str],
        condition: str = "1=1",
        page_size: int = 5,
    ):
        page = 1
        columns = (
            "*"
            if columns_list
            else ",".join([column.join(["`", "`"]) for column in columns_list])
        )
        SQL_STATEMENT = f"SELECT {columns} FROM {tableName} WHERE {condition}"
        with self.connector.cursor(prepared=True, buffered=False) as cursor:
            while True:
                cursor.execute(
                    SQL_STATEMENT + f" LIMIT {page_size} OFFSET {page*page_size}"
                )
                chunk = cursor.fetchall()
                if not chunk:
                    break

                for i in chunk:
                    print(i)
                page += 1

    def close(self) -> None:
        return self.connector.close()


class MysqlConnectPool(ConnectPool):
    '''
    自定义MYSQL连接池
    '''

    def __init__(
        self,
        pool_name: str,
        size: int,
        pool_reset_session: bool = True,
        **dbconfig: dict[str, str],
    ):
        super().__init__(pool_name, size, **dbconfig)
        self.pool = MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=size,
            pool_reset_session=pool_reset_session,
            **dbconfig,
        )
        print(f"初始化MYSQL连接池:连接池名称:{pool_name},连接池并发数量:{size},连接配置：{dbconfig}")

    def batch_query(self,):
        pass

    def __enter__(self,):
        return self
    

    def __exit__(self,):
        del self.pool



if __name__ == "__main__":
    mysqlCon = MysqlConnector("localhost", 3306, "leo", "leo130")
    print(mysqlCon.testConnect())
    mysqlCon.query("employees.dept_manager", ["*"])
    mysqlCon.close()
