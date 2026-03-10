import pymysql # pyright: ignore[reportMissingModuleSource]
from pymysql import OperationalError, ProgrammingError # pyright: ignore[reportMissingModuleSource]



DB_CONFIG = {
    "sit_remit_bank": {  # 汇出行数据库
        "host": "devandsitmysql.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "db": "remi-highsun-out",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl" : {
            "verify_cert": False,  # 不验证服务器证书
            "check_hostname": False  # 不验证主机名（Azure MySQL 建议加这个）
        }  
    },
    "sit_receive_bank": {  # 汇入行数据库
        "host": "devandsitmysql.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "db": "remi-highsun-in",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl" : {
            "verify_cert": False,  # 不验证服务器证书
            "check_hostname": False  # 不验证主机名（Azure MySQL 建议加这个）
        }  # 关键：开启SSL加密，无CA时跳过证书验证
    },
    "sit_admin" : {  # 汇入行数据库
        "host": "devandsitmysql.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "db": "remi-admin",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl" : {
            "verify_cert": False,  # 不验证服务器证书
            "check_hostname": False  # 不验证主机名（Azure MySQL 建议加这个）
        }  # 关键：开启SSL加密，无CA时跳过证书验证
    },
        "uat_remit_bank": {  # 汇出行数据库
        "host": "remisandbox.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "db": "remi-highsun-out",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl" : {
            "verify_cert": False,  # 不验证服务器证书
            "check_hostname": False  # 不验证主机名（Azure MySQL 建议加这个）
        }  
    },
    "uat_receive_bank": {  # 汇入行数据库
        "host": "remisandbox.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "db": "remi-highsun-in",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl" : {
            "verify_cert": False,  # 不验证服务器证书
            "check_hostname": False  # 不验证主机名（Azure MySQL 建议加这个）
        }  # 关键：开启SSL加密，无CA时跳过证书验证
    },
    "uat_fxa_bank": {  # 汇入行数据库
        "host": "remisandbox.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "db": "remi-fx-a",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl" : {
            "verify_cert": False,  # 不验证服务器证书
            "check_hostname": False  # 不验证主机名（Azure MySQL 建议加这个）
        }  # 关键：开启SSL加密，无CA时跳过证书验证
    },
    "uat_fxb_bank": {  # 汇入行数据库
        "host": "remisandbox.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "db": "remi-fx-b",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl" : {
            "verify_cert": False,  # 不验证服务器证书
            "check_hostname": False  # 不验证主机名（Azure MySQL 建议加这个）
        }  # 关键：开启SSL加密，无CA时跳过证书验证
    }
}


def get_db_conn(db_type):
    """获取指定数据库连接"""
    config = DB_CONFIG.get(db_type)
    if not config:
        raise ValueError(f"无效数据库类型：{db_type}，可选：sit_remit_bank/sit_receive_bank")
    try:
        conn = pymysql.connect(**config)
        return conn
    except OperationalError as e:
        raise Exception(f"{db_type}数据库连接失败：{str(e)}")
    



def query_single_bank_addresses(db_type):
    """查询单个数据库（汇出行/汇入行）的address_info表，返回地址映射"""
    conn = get_db_conn(db_type)
    cursor = None
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT v.belong_to , a.network , a.coin_id,  a.business_type , a.address FROM address_info a JOIN vault_info v ON a.vault_code = v.vault_code")
        addresses = cursor.fetchall()
        return addresses 
    except ProgrammingError as e:
        raise Exception(f"查询{db_type}的address_info表失败：{str(e)}")
    finally:
        if cursor:
            cursor.close()
        conn.close()

def get_all_addresses():
    """汇总汇出行+汇入行的地址信息"""
    remit_addresses = query_single_bank_addresses("sit_remit_bank")
    receive_addresses = query_single_bank_addresses("sit_receive_bank")
    all_addresses = remit_addresses + receive_addresses

    return all_addresses


def get_read_record(db_type,service_uni_id):
    """获取 read 记录"""
    conn = get_db_conn(db_type)
    cursor = None
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        sql = f"SELECT service_uni_id , status FROM read_record WHERE service_uni_id = '{service_uni_id}'"
        cursor.execute(sql)
        read_record = cursor.fetchone()
        return read_record
    except ProgrammingError as e:
        raise Exception(f"查询{db_type}的read_record表失败：{str(e)}")
    finally:
        if cursor:
            cursor.close()
        conn.close()




def get_transaction_record(db_type,bank_req_id,order_id):
    """获取交易记录"""
    conn = get_db_conn(db_type)
    cursor = None
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        if bank_req_id != "":
            sql = f"SELECT m.order_id trans_out_order_id,m.bank_req_id,m.status_flow,t.order_id  two_step_order_id  FROM message_transaction_record m LEFT JOIN two_step_txn_record t ON t.origin_order_id=m.order_id WHERE m.bank_req_id ='{bank_req_id}'"
        else:
            sql = f"SELECT order_id,status_flow FROM message_transaction_record WHERE order_id = '{order_id}'"
        cursor.execute(sql)
        record = cursor.fetchone()
        return record
    except ProgrammingError as e:
        raise Exception(f"查询{db_type}的message_transaction_record表失败：{str(e)}")
    finally:
        if cursor:
            cursor.close()
        conn.close()



def get_all_inward_processing_order(db_type):
    """获取所有待处理的汇入"""
    conn = get_db_conn(db_type)
    cursor = None
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        sql = f"SELECT bank_req_id FROM message_transaction_record WHERE status_flow = 'IN_RESULT_PROCESSING'"
        cursor.execute(sql)
        record = cursor.fetchall()
        return record
    except ProgrammingError as e:
        raise Exception(f"查询{db_type}的message_transaction_record表失败：{str(e)}")
    finally:
        if cursor:
            cursor.close()
        conn.close()




if __name__ == "__main__":
    # print(get_transaction_record("receive_bank", "MT10320251215-152424-0559055" , ""))
    all_result = get_all_inward_processing_order("uat_receive_bank")

    for order in all_result:
        print(f"order:{order}")
        bank_req_id = order['bank_req_id']
        print(f"bank_req_id:{bank_req_id}")













