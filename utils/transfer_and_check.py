from time import sleep
from typing import Dict, Optional, Any, Tuple
import pprint
from utils.create_transaction_MT103 import  inward_result_update
from utils.query_db_data import get_read_record, get_transaction_record

# 配置常量
MAX_RETRY_TIMES = 60
RETRY_INTERVAL = 5  # 秒


class TransactionChecker:
    """跨行转账交易校验类"""
    
    def __init__(self, service_uni_id_103: str, bank_req_id: str):
        self.service_uni_id_103 = service_uni_id_103
        self.bank_req_id = bank_req_id
        # 初始化校验结果
        self.check_result: Dict[str, Any] = {
            "service_uni_id_103": service_uni_id_103,
            "bank_req_id": bank_req_id,
            "overall_status": "PASS",
            "check_details": []
        }

    def _retry_query(self, query_func, *args, **kwargs) -> Optional[Any]:
        """
        通用重试查询方法
        :param query_func: 查询函数
        :param args: 查询函数位置参数
        :param kwargs: 查询函数关键字参数
        :return: 查询结果，None表示重试后仍无结果
        """
        for i in range(MAX_RETRY_TIMES):
            sleep(RETRY_INTERVAL)
            try:
                result = query_func(*args, **kwargs)
                print(f"第{i+1}次查询{query_func.__name__}结果: {result}")
                if result is not None:
                    return result
            except Exception as e:
                print(f"第{i+1}次查询{query_func.__name__}异常: {str(e)}")
        
        print(f"重试{MAX_RETRY_TIMES}次后{query_func.__name__}仍无结果")
        return None

    def _check_status(self, step: int, name: str, current_status: str, 
                     expected_status: str, allow_none: bool = False) -> None:
        """
        通用状态校验方法
        :param step: 校验步骤
        :param name: 校验名称
        :param current_status: 当前状态
        :param expected_status: 预期状态
        :param allow_none: 是否允许无记录（None）
        """
        detail = {"step": step, "name": name}
        
        # 处理无记录情况
        if current_status is None:
            if not allow_none:
                detail["msg"] = "无记录"
                detail["status"] = "FAIL"
                self._update_check_result(detail, "FAIL")
            return
        
        # 状态校验
        if current_status == expected_status:
            detail["status"] = "PASS"
            detail["msg"] = f"校验通过（状态{expected_status}）"
            self.check_result["check_details"].append(detail)
        else:
            detail["status"] = "FAIL"
            detail["msg"] = f"状态异常：{current_status}（预期{expected_status}）"
            self._update_check_result(detail, "FAIL")
        # print(f"check_result:{self.check_result}")

    def _update_check_result(self, detail: Dict[str, Any], overall_status: str) -> None:
        """更新校验结果"""
        self.check_result["check_details"].append(detail)
        if self.check_result["overall_status"] == "PASS":
            self.check_result["overall_status"] = overall_status

    def _handle_exception(self, step: int, name: str, e: Exception) -> None:
        """统一异常处理"""
        detail = {
            "step": step,
            "name": name,
            "status": "ERROR",
            "msg": str(e)
        }
        self.check_result["check_details"].append(detail)
        self.check_result["overall_status"] = "ERROR"

    def check_read_record(self) -> None:
        """校验1: 汇出行read_record状态"""

        try:
            for i in range(MAX_RETRY_TIMES):
                sleep(RETRY_INTERVAL)
                read_record = get_read_record("remit_bank", self.service_uni_id_103)
                print(f"第{i+1}次查询read_record: {read_record}")
                
                if read_record is None:
                    if i == MAX_RETRY_TIMES - 1:
                        self._check_status(1, "汇出行-read_record", None, "RETURNED_ACK")
                elif read_record["status"] == "RETURNED_NAK":
                    self._check_status(1, "汇出行-read_record", "RETURNED_NAK", "RETURNED_ACK")
                    break
                elif read_record["status"] == "RETURNED_ACK":
                    self._check_status(1, "汇出行-read_record", "RETURNED_ACK", "RETURNED_ACK")
                    break
                elif i == MAX_RETRY_TIMES - 1:
                    self._check_status(1, "汇出行-read_record", read_record["status"], "RETURNED_ACK")
        except Exception as e:
            self._handle_exception(1, "汇出行-read_record", e)


 

    def check_out_bank_transaction(self) -> Optional[Dict[str, Any]]:
        """校验3: 汇出行message_transaction_record状态"""
        out_record = None
        try:
            for i in range(MAX_RETRY_TIMES):
                sleep(RETRY_INTERVAL)
                out_record = get_transaction_record("remit_bank", self.bank_req_id, "")
                print(f"self.bank_req_id:{self.bank_req_id}")
                print(f"第{i+1}次查询out_message_transaction_record: {out_record}")
                two_step_order_id = out_record.get("two_step_order_id")
                
                
                if out_record is None:
                    if i == MAX_RETRY_TIMES - 1:
                        self._check_status(3, "汇出行-message_tansaction_record", None, "OUT_DONE_BACK_ACK") 
                else: 
                    """校验2: 汇出行内部资金划拨记录"""   
                    if not two_step_order_id:
                        detail = {
                            "step": 2,
                            "name": "汇出行-message_tansaction_record",
                            "msg": "跨行转账没有产生内部资金划拨"
                        }
                        self.check_result["check_details"].append(detail)
                    else:
                        try:
                            for i in range(MAX_RETRY_TIMES):
                                sleep(RETRY_INTERVAL)
                                fund_record = get_transaction_record("remit_bank", "", two_step_order_id)
                                print(f"第{i+1}次查询资金从Liquidity到Pay记录: {fund_record}")
                                
                                if fund_record is None:
                                    if i == MAX_RETRY_TIMES - 1:
                                        self._check_status(2, "汇出行-message_tansaction_record", None, "OUT_DONE_SUCCESS")
                                elif fund_record["status_flow"] == "OUT_DONE_SUCCESS":
                                    self._check_status(2, "汇出行-message_tansaction_record", "OUT_DONE_SUCCESS", "OUT_DONE_SUCCESS")
                                    break
                                elif i == MAX_RETRY_TIMES - 1:
                                    self._check_status(2, "汇出行-message_tansaction_record", fund_record["status_flow"], "OUT_DONE_SUCCESS")
                                    
                        except Exception as e:
                            self._handle_exception(2, "汇出行-message_transaction_record", e)
                            break

                    if self.check_result.get("overall_status")  != "FAIL":       
                        if out_record["status_flow"] == "OUT_DONE_BACK_NAK":
                            self._check_status(3, "汇出行-message_tansaction_record", "OUT_DONE_BACK_NAK", "OUT_DONE_BACK_ACK")
                            break
                        elif out_record["status_flow"] == "OUT_DONE_BACK_ACK":
                            self._check_status(3, "汇出行-message_tansaction_record", "OUT_DONE_BACK_ACK", "OUT_DONE_BACK_ACK")
                            break
                        elif i == MAX_RETRY_TIMES - 1:
                            self._check_status(3, "汇出行-message_tansaction_record", out_record["status_flow"], "OUT_DONE_BACK_ACK")
                    else:
                        break 
        except Exception as e:
            self._handle_exception(3, "汇出行-message_transaction_record", e)



    def check_in_bank_transaction(self) -> None:
        """校验4: 汇入行message_transaction_record状态"""
        try:
            for i in range(MAX_RETRY_TIMES):
                sleep(RETRY_INTERVAL)
                in_record = get_transaction_record("receive_bank", self.bank_req_id, "")
                print(f"第{i+1}次查询in_message_transaction_record: {in_record}")
                
                if in_record is None:
                    if i == MAX_RETRY_TIMES - 1:
                        self._check_status(4, "汇入行-message_tansaction_record", None, "IN_RESULT_PROCESSING")
                elif in_record["status_flow"] == "IN_RESULT_PROCESSING":
                    self._check_status(4, "汇入行-message_tansaction_record", "IN_RESULT_PROCESSING", "IN_RESULT_PROCESSING")
                    break
                elif i == MAX_RETRY_TIMES - 1:
                    self._check_status(4, "汇入行-message_tansaction_record", in_record["status_flow"], "IN_RESULT_PROCESSING")
        except Exception as e:
            self._handle_exception(4, "汇入行-message_transaction_record", e)

    def process_inward_result(self, result: int, reject_reason: str) -> None:
        """
        处理汇入结果（同意/拒绝）
        :param result: 0-同意，非0-拒绝
        :param reject_reason: 拒绝原因
        """
        # 调用汇入结果更新接口
        try:
            response = inward_result_update(self.bank_req_id, result, reject_reason)
            if response.status_code != 200:
                detail = {
                    "step": 5,
                    "name": "调用汇入处理inwardResultUpdate接口异常",
                    "msg": f"接口返回状态码: {response.status_code}"
                }
                self._update_check_result(detail, "ERROR")
                return
        except Exception as e:
            self._handle_exception(5, "调用汇入处理inwardResultUpdate接口", e)
            return

        # 同意汇入的校验
        if result == 0:
            self._check_agree_inward()
        # 拒绝汇入的校验
        else:
            self._check_reject_inward()

    def _check_agree_inward(self) -> None:
        """校验5: 同意汇入后的状态"""
        try:
            for i in range(MAX_RETRY_TIMES):
                sleep(RETRY_INTERVAL)
                in_record = get_transaction_record("receive_bank", self.bank_req_id, "")
                print(f"同意汇入后，第{i+1}次查询in_message_transaction_record: {in_record}")
                
                if in_record and in_record["status_flow"] == "IN_DONE":
                    self._check_status(5, "汇入行-message_tansaction_record", "IN_DONE", "IN_DONE")
                    break
                elif i == MAX_RETRY_TIMES - 1:
                    current_status = in_record["status_flow"] if in_record else None
                    self._check_status(5, "汇入行-message_tansaction_record", current_status, "IN_DONE")
        except Exception as e:
            self._handle_exception(5, "汇入行-message_transaction_record", e)

    def _check_reject_inward(self) -> None:
        """校验5: 拒绝汇入后的退款状态"""
        refund_bank_req_id = f"{self.bank_req_id}_R1"
        try:
            for i in range(MAX_RETRY_TIMES):
                sleep(RETRY_INTERVAL)
                refund_record = get_transaction_record("remit_bank", refund_bank_req_id, "")
                print(f"拒绝汇入后，第{i+1}次查询退款记录: {refund_record}")
                
                if refund_record is None:
                    if i == MAX_RETRY_TIMES - 1:
                        self._check_status(5, "汇出行-退款汇入-message_tansaction_record", None, "IN_DONE")
                elif refund_record["status_flow"] == "IN_DONE":
                    self._check_status(5, "汇出行-退款汇入-message_tansaction_record", "IN_DONE", "IN_DONE")
                    break
                elif i == MAX_RETRY_TIMES - 1:
                    self._check_status(5, "汇出行-退款汇入-message_tansaction_record", refund_record["status_flow"], "IN_DONE")
        except Exception as e:
            self._handle_exception(5, "汇出行-退款汇入-message_transaction_record", e)


    def is_status_ok(self) -> bool:
            """检查整体状态是否不是FAIL"""
            status = self.check_result.get("overall_status")
            # print(f"overall_status:{status}")
            return status != "FAIL"

    def run_all_checks(self, result: int = 0, reject_reason: str = "") -> Dict[str, Any]:
        """执行所有校验步骤"""

        # print(f"执行结果检查前,检查参数,result:{result},reject_reason:{reject_reason}")

        
        # 步骤1: 校验read_record
        if self.is_status_ok():
            read_record = self.check_read_record()

        # 步骤2: 校验汇出行资金划拨
        # 步骤3: 校验汇出行交易记录
        if self.is_status_ok():
            out_record = self.check_out_bank_transaction()


        # 步骤4: 校验汇入行交易记录
        if self.is_status_ok():
            self.check_in_bank_transaction()

        # 步骤5: 处理汇入结果并校验
        if self.is_status_ok():
            self.process_inward_result(result, reject_reason)
        

        return self.check_result


def cross_bank_transfer_and_check(
    service_uni_id_103: str,
    bank_req_id: str,
    result: int = 0,
    reject_reason: str = ""
) -> Dict[str, Any]:
    """
    跨行转账校验主函数（对外接口）
    :param service_uni_id_103: 103交易唯一ID
    :param bank_req_id: 银行请求ID
    :param result: 汇入结果（0-同意，非0-拒绝）
    :param reject_reason: 拒绝原因
    :return: 校验结果
    """
    checker = TransactionChecker(service_uni_id_103, bank_req_id)
    check_result = checker.run_all_checks(result, reject_reason)
    import json
    beautiful_json = json.dumps(check_result, ensure_ascii=False, indent=2, sort_keys=False)
    print(beautiful_json)
    # print(f"check_result:{check_result}")
    return check_result


# # 测试入口
# if __name__ == "__main__":
#     # 示例调用
#     result = cross_bank_transfer_and_check("b231713c2454730d22c497254d30d6f2", "MT10320251217-142027-0934439", 0, "")
#     print("最终校验结果:")
#     pprint.pprint(result, indent=2, width=200)