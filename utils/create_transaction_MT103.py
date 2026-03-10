import requests # type: ignore
import uuid
import random
from datetime import datetime
import jsonpath # type: ignore

# -------------------------- 核心参数生成函数 --------------------------
def generate_time_str(format_str: str) -> str:
    """生成指定格式的时间字符串（兼容JMeter的__time语法）"""
    now = datetime.now()
    # 映射JMeter时间格式到Python strftime格式
    format_mapping = {
        "yyMMdd": "%y%m%d",
        "yyyyMMdd-HHmmss": "%Y%m%d-%H%M%S"
    }
    python_format = format_mapping.get(format_str, format_str)
    return now.strftime(python_format)

def generate_random_num(min_val: int, max_val: int) -> int:
    """生成指定范围的随机数（兼容JMeter的__Random语法）"""
    return random.randint(min_val, max_val)

def generate_uuid() -> str:
    """生成UUID（兼容JMeter的java.util.UUID.randomUUID()）"""
    return str(uuid.uuid4())

# -------------------------- 固定参数定义 --------------------------
SIT_OUT_BANK_HOST = "http://remi-prep-service-out-sit.remitech.ai"
SIT_IN_BANK_HOST = "http://remi-prep-service-in-sit.remitech.ai"
UAT_OUT_BANK_HOST = "http://20.212.242.52:30084"
UAT_IN_BANK_HOST = "http://20.212.242.52:30083"
UAT_FXA_BANK_HOST = "http://20.212.242.52:30085"
UAT_FXB_BANK_HOST = "http://20.212.242.52:30086"
SEND_MESSAGE_URI = "/v1/api/prep/sendMessage"
INWARD_RESULT_UPDATE_URI = "/v1/api/prep/inwardResultUpdate"
ORG_OUT_REMI = "AGPBLALAXXX"  # org_out_remi固定值
ORG_IN_REMI = "COEBLALALNT"   # org_in_remi固定值
CURRENCY = "HKTSIT"           # currency固定值
OUT_REMI_WITH_PRIORITY = "AGPBLALANXXX"
IN_REMI_WITH_PRIORITY = "COEBLALANLNT"

# -------------------------- 动态参数生成 --------------------------

def generate_103_message():
    # 1. 基础时间戳（统一用当前时间，避免多次生成不一致）
    time_yyMMdd = generate_time_str("yyMMdd")
    time_yyyyMMdd_HHmmss = generate_time_str("yyyyMMdd-HHmmss")
    # 2. 核心动态参数
    requestId = generate_uuid()
    reference_103 = f"MT103{time_yyyyMMdd_HHmmss}-0{generate_random_num(0, 1000000)}"
    amount = generate_random_num(1, 5000)  # 最终使用5000范围的amount（覆盖1-10的初始定义）


    # 3.生成 103 报文请求体
    message = f"""\
{{1:F01{OUT_REMI_WITH_PRIORITY}0000000000}}
{{2:I103{IN_REMI_WITH_PRIORITY}N}}
{{3:{{108:2022033100001579}}}}
{{4:
:20:{reference_103}
:23B:CRED
:32A:{time_yyMMdd}{CURRENCY}{amount}
:50K:/0000100110005690
LANCER SMITH MR
SOPHOUN VILLAGE, MAI DISTRICT,
PHONGSALY PROVINCE, LAOS
:57A:COEBLALALNT
:59:/401000201000003795
R5R43RFEW
:70:/ROC/
:71A:SHA
:72:MT103测试MT103测试MT103测试MT103测试MT103测试MT103测试MT103测试
{ORG_OUT_REMI}0000000
{ORG_IN_REMI}0000000
-}}
"""
    

    #生成 103 报文请求体
    request_body = {
        "commonReq": {
            "apiKey": "95e12219871113f500f7e1d7f0f12c5f",
            "clientReqTime": datetime.now().isoformat(),  # 匹配原格式2025-10-22T10:03:41.733597
            "requestId": requestId
        },
        "messageReq": {
            "message": f"{message}"
        }
    }
    print(request_body)

    return request_body,reference_103






# -------------------------- 发送接口请求 --------------------------
def send_103_transfer():
    generate_result = generate_103_message()
    request_body = generate_result[0]
    reference_103 = generate_result[1]



    """发送POST请求到目标接口"""
    # 替换为实际的接口URL
    api_url = UAT_OUT_BANK_HOST + SEND_MESSAGE_URI
    headers = {
        "Content-Type": "application/json",  # 接口通常为JSON格式
        # 如需其他请求头（如token、Authorization），可在此添加
    }

    try:
        # 发送POST请求
        message_response = requests.post(
            url=api_url,
            json=request_body,  # 自动序列化JSON并设置Content-Type
            headers=headers,
            timeout=30  # 请求超时时间
        )

        # # 打印请求详情
        # print("===== 请求信息 =====")
        # print(f"请求URL: {api_url}")
        # print(f"请求头: {headers}")
        # print(f"请求体: {request_body}")

        # # 打印响应详情
        # print("\n===== 响应信息 =====")
        # print(f"响应状态码: {message_response.status_code}")
        # print(f"响应内容: {message_response.text}")
        service_uni_id = jsonpath.jsonpath(message_response.json(),f"$..data")[0]
        
        return service_uni_id,reference_103,message_response.status_code

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {str(e)}")
        return None
    

def inward_result_update(bank_req_id , result , reject_reason="",bank_role = "sit_receive_bank"):  
    if bank_role == "sit_receive_bank":
        api_url = SIT_IN_BANK_HOST + INWARD_RESULT_UPDATE_URI
    elif bank_role == "uat_receive_bank":
        api_url = UAT_IN_BANK_HOST + INWARD_RESULT_UPDATE_URI
    elif bank_role == "sit_remit_bank":
        api_url = SIT_OUT_BANK_HOST + INWARD_RESULT_UPDATE_URI
    elif  bank_role == "uat_remit_bank":
        api_url = UAT_OUT_BANK_HOST + INWARD_RESULT_UPDATE_URI
    elif  bank_role == "uat_fxa_bank":
        api_url = UAT_FXA_BANK_HOST + INWARD_RESULT_UPDATE_URI
    elif  bank_role == "uat_fxb_bank":
        api_url = UAT_FXB_BANK_HOST + INWARD_RESULT_UPDATE_URI
    else:
        pass
    headers = {
        "Content-Type": "application/json",  # 接口通常为JSON格式
        # 如需其他请求头（如token、Authorization），可在此添加
    }

    requestId = generate_uuid()

    request_body = {
    "receiveResultReq": {
    "transReqId": bank_req_id ,
    "result": result ,
    "rejectReason": reject_reason 
    },
    "commonReq": {
    "requestId": requestId ,
    "clientReqTime": "2025-06-25T17:10:02.331175",
    "apiKey": "95e12219871113f500f7e1d7f0f12c5f"
    }
    }
    update_response = requests.post(
            url=api_url,
            json=request_body,  # 自动序列化JSON并设置Content-Type
            headers=headers,
            timeout=30  # 请求超时时间
        )

    # # 打印请求详情
    # print("===== 请求信息 =====")
    # print(f"请求URL: {api_url}")
    # print(f"请求头: {headers}")
    # print(f"请求体: {request_body}")

    # 打印响应详情
    # print("\n===== 响应信息 =====")
    # print(f"响应状态码: {update_response.status_code}")
    # print(f"响应内容: {update_response.text}")
    return update_response




# 执行请求
if __name__ == "__main__":
    send_103_transfer()

