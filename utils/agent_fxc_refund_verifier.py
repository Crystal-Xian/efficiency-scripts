import time
import requests
from typing import Optional

from utils.agent_base_verifier import AgentBaseVerifier
from utils.query_db_data import get_db_conn


FXC_REFUND_API = "http://remi-prep-service-fxc-sit.remitech.ai/v1/api/prep/inwardResultUpdate"
FXC_REFUND_API_KEY = "95e12219871113f500f7e1d7f0f12c5f"


class AgentFxcRefundVerifier(AgentBaseVerifier):

    def __init__(
        self,
        db_key: str,           # e.g. "sit_fxc"
        bank_req_id: str,       # 原汇入的 bank_req_id
        amount: float,
        coin_id: str,
        result: int = 1,       # 1=同意, 0=拒绝
        reject_reason: str = "high risk",
        sender_remi_id: str = "FXTREGSFBBB",  # FXB's BIC
        report=None,
        timeout: int = 120,
    ):
        super().__init__(db_key=db_key, report=report)
        self.bank_req_id = bank_req_id
        self.amount = amount
        self.coin_id = coin_id
        self.result = result
        self.reject_reason = reject_reason
        self.sender_remi_id = sender_remi_id
        self.base_timeout_s = timeout
        self.start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self._refund_order_id: str = ""   # FXC 内部退款 order_id

    def _call_inward_result_update(self) -> tuple:
        request_id = f"{self.bank_req_id}_R{int(time.time())}"
        payload = {
            "receiveResultReq": {
                "transReqId": self.bank_req_id,
                "result": self.result,
                "rejectReason": self.reject_reason,
                "senderRemiId": self.sender_remi_id,
            },
            "commonReq": {
                "requestId": request_id,
                "clientReqTime": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                "apiKey": FXC_REFUND_API_KEY,
            },
        }
        resp = requests.post(FXC_REFUND_API, json=payload, timeout=30)
        print(f"[HTTP] inwardResultUpdate status={resp.status_code}")
        if self.report:
            self.report.add_http_call(
                "inwardResultUpdate", "POST", FXC_REFUND_API,
                {"Content-Type": "application/json"},
                payload, resp.status_code, resp.text[:500],
            )
        return resp.status_code, resp.text, request_id

    def _get_refund_stage1(self) -> Optional[dict]:
        sql = (
            "SELECT id, order_id, business_type, status_flow, amount, coin_id, "
            "       returned_base_order_id, direction "
            "FROM message_transaction_record "
            "WHERE returned_base_order_id = %s "
            "  AND business_type = 'REFUND_STAGE1_TRANSFER' "
            "  AND direction = 'OUT' "
            "  AND coin_id = %s "
            "ORDER BY id DESC LIMIT 1"
        )
        row = self._db_query(sql, (self._refund_order_id, self.coin_id))
        if self.report:
            self.report.db_queries.append({
                "db_type": self.db_key,
                "action": "REFUND_STAGE1_TRANSFER",
                "hit": row is not None,
                "status_flow": str(row.get("status_flow")) if row else "",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        return row

    def _get_return_trans_in(self) -> Optional[dict]:
        sql = (
            "SELECT id, order_id, business_type, status_flow, amount, coin_id, "
            "       returned_base_order_id, direction, tx_id "
            "FROM message_transaction_record "
            "WHERE returned_base_order_id = %s "
            "  AND business_type = 'RETURN_TRANS_IN' "
            "  AND direction = 'OUT' "
            "  AND coin_id = %s "
            "ORDER BY id DESC LIMIT 1"
        )
        row = self._db_query(sql, (self._refund_order_id, self.coin_id))
        if self.report:
            self.report.db_queries.append({
                "db_type": self.db_key,
                "action": "RETURN_TRANS_IN",
                "hit": row is not None,
                "status_flow": str(row.get("status_flow")) if row else "",
                "tx_id": str(row.get("tx_id")) if row else "",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        return row

    def verify_all(self, timeout: Optional[int] = None, interval: Optional[int] = None):
        timeout_s = timeout if timeout is not None else self.base_timeout_s
        interval_s = interval if interval is not None else self.es_interval_s
        deadline = time.time() + timeout_s

        print(f"[FXC 退款验证] bank_req_id={self.bank_req_id}, "
              f"amount={self.amount} {self.coin_id}, timeout={timeout_s}s")

        status, body, request_id = self._call_inward_result_update()
        if status != 200 or (body and '"code":0' not in body):
            self.log_step(1, "inwardResultUpdate API", "FAIL",
                          f"HTTP {status}: {body[:200]}")
            return False

        self._refund_order_id = request_id.split("_")[0]
        self.log_step(1, "inwardResultUpdate API", "PASS",
                      f"请求ID={request_id}")

        es_deadline = time.time() + 60
        self._check_es_log("updateOnchainStatus",
                            self.bank_req_id, es_deadline)

        elapsed = 0
        step1_pass = False
        step2_pass = False

        while time.time() < deadline:
            stage1 = self._get_refund_stage1()
            if stage1:
                stage1_ok = (
                    stage1["status_flow"] == "OUT_DONE_SUCCESS" and
                    float(stage1["amount"]) == float(self.amount)
                )
                self.log_step(
                    2, "REFUND_STAGE1_TRANSFER", "PASS" if stage1_ok else "FAIL",
                    f"order_id={stage1['order_id']}, status={stage1['status_flow']}, "
                    f"amount={stage1['amount']} {self.coin_id}",
                    raw_data=stage1,
                )
                step1_pass = stage1_ok
                if stage1_ok:
                    return_tx = self._get_return_trans_in()
                    if return_tx:
                        return_tx_ok = (
                            return_tx["status_flow"] == "OUT_DONE_BACK_ACK" and
                            float(return_tx["amount"]) == float(self.amount)
                        )
                        self.log_step(
                            3, "RETURN_TRANS_IN", "PASS" if return_tx_ok else "FAIL",
                            f"order_id={return_tx['order_id']}, status={return_tx['status_flow']}, "
                            f"amount={return_tx['amount']}",
                            raw_data=return_tx,
                        )
                        step2_pass = return_tx_ok
                    else:
                        self.log_step(3, "RETURN_TRANS_IN", "TIMEOUT",
                                      "超时未查到 RETURN_TRANS_IN 记录")
                    break
            time.sleep(interval_s)
            elapsed = int(time.time() - (deadline - timeout_s))
            print(f"[轮询] {elapsed}s, 等待 REFUND_STAGE1_TRANSFER ...")

        if not step1_pass and stage1 is None:
            self.log_step(2, "REFUND_STAGE1_TRANSFER", "TIMEOUT",
                          "超时未查到 REFUND_STAGE1_TRANSFER 记录")
        if not step2_pass and not step1_pass:
            return False
        return step1_pass and step2_pass
