from utils.create_transaction_MT103 import send_103_transfer
from utils.transfer_and_check import cross_bank_transfer_and_check


test_cases = [{"result" : 1 , "reject_reason" : "high risk"}]


for i in test_cases:
    result , reject_reason = i["result"] , i["reject_reason"]
    try:
        send_message_result = send_103_transfer()
        service_uni_id_103 = send_message_result[0]
        bank_req_id = send_message_result[1]
        code = send_message_result[2]
        # print(f"service_uni_id_103:{service_uni_id_103},bank_req_id:{bank_req_id},code:{code}")
    except Exception as e:
        raise Exception(f"发起 103报文转账 失败：{str(e)}")

    if code == 200:
        cross_bank_transfer_and_check(service_uni_id_103,bank_req_id,result,reject_reason)
    else:
            check_result = {
    "bank_req_id": bank_req_id,
    "overall_status": "FAIL",
    "check_details": [{"name": "请求 send_message 接口", "status": "ERROR", "msg": "接口请求失败"}]
    }

