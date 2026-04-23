import time
from typing import Optional

from utils.agent_base_verifier import AgentBaseVerifier


class AgentFxbRefundVerifier(AgentBaseVerifier):

    def __init__(
        self,
        db_key: str,            # e.g. "sit_fxb"
        original_bank_req_id: str,   # 原汇出 bank_req_id
        refund_bank_req_id: str,     # 退款 bank_req_id = original + "_R1"
        amount: float,
        coin_id: str,
        report=None,
        timeout: int = 180,
    ):
        super().__init__(db_key=db_key, report=report)
        self.original_bank_req_id = original_bank_req_id
        self.refund_bank_req_id = refund_bank_req_id
        self.amount = amount
        self.coin_id = coin_id
        self.base_timeout_s = timeout
        self.start_time = time.strftime("%Y-%m-%d %H:%M:%S")

    def _get_return_trans_out(self) -> Optional[dict]:
        sql = (
            "SELECT id, order_id, business_type, status_flow, amount, coin_id, "
            "       bank_req_id, sender_remi_id, receive_remi_id, direction, tx_id "
            "FROM message_transaction_record "
            "WHERE bank_req_id = %s "
            "  AND business_type = 'RETURN_TRANS_OUT' "
            "  AND direction = 'IN' "
            "ORDER BY id DESC LIMIT 1"
        )
        row = self._db_query(sql, (self.refund_bank_req_id,))
        if self.report:
            self.report.db_queries.append({
                "db_type": self.db_key,
                "action": "RETURN_TRANS_OUT",
                "hit": row is not None,
                "status_flow": str(row.get("status_flow")) if row else "",
                "bank_req_id": self.refund_bank_req_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        return row

    def _get_auto_transfer_inner(self, return_order_id: str) -> Optional[dict]:
        sql = (
            "SELECT id, order_id, business_type, status_flow, amount, coin_id, "
            "       direction, address, cp_address "
            "FROM message_transaction_record "
            "WHERE order_id = %s "
            "  AND business_type = 'AUTO_TRANSFER_INNER' "
            "ORDER BY id DESC LIMIT 1"
        )
        row = self._db_query(sql, (return_order_id + "_INTERNAL",))
        if self.report:
            self.report.db_queries.append({
                "db_type": self.db_key,
                "action": "AUTO_TRANSFER_INNER",
                "hit": row is not None,
                "status_flow": str(row.get("status_flow")) if row else "",
                "order_id": return_order_id + "_INTERNAL",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        return row

    def verify_all(self, timeout: Optional[int] = None, interval: Optional[int] = None):
        timeout_s = timeout if timeout is not None else self.base_timeout_s
        interval_s = interval if interval is not None else self.es_interval_s
        deadline = time.time() + timeout_s

        print(f"[FXB 退款汇入验证] refund_bank_req_id={self.refund_bank_req_id}, "
              f"amount={self.amount} {self.coin_id}, timeout={timeout_s}s")

        self._check_es_log("inwardMessageReceive",
                           self.original_bank_req_id, deadline)

        step1_pass = False
        step2_pass = False
        return_record = None

        while time.time() < deadline:
            return_record = self._get_return_trans_out()
            if return_record:
                return_ok = (
                    return_record["status_flow"] == "IN_DONE" and
                    float(return_record["amount"]) == float(self.amount)
                )
                self.log_step(
                    1, "RETURN_TRANS_OUT (退款汇入)", "PASS" if return_ok else "FAIL",
                    f"order_id={return_record['order_id']}, status={return_record['status_flow']}, "
                    f"amount={return_record['amount']}",
                    raw_data=return_record,
                )
                step1_pass = return_ok
                if return_ok:
                    inner = self._get_auto_transfer_inner(return_record["order_id"])
                    if inner:
                        inner_ok = (
                            inner["status_flow"] == "OUT_SEND_SUCCESS" and
                            float(inner["amount"]) == float(self.amount)
                        )
                        self.log_step(
                            2, "AUTO_TRANSFER_INNER (内部划拨)", "PASS" if inner_ok else "FAIL",
                            f"order_id={inner['order_id']}, status={inner['status_flow']}, "
                            f"amount={inner['amount']}",
                            raw_data=inner,
                        )
                        step2_pass = inner_ok
                    else:
                        self.log_step(2, "AUTO_TRANSFER_INNER", "TIMEOUT",
                                      "超时未查到 AUTO_TRANSFER_INNER 记录")
                break
            elapsed = int(time.time() - (deadline - timeout_s))
            print(f"[轮询] {elapsed}s, 等待 RETURN_TRANS_OUT ...")
            time.sleep(interval_s)

        if not step1_pass and return_record is None:
            self.log_step(1, "RETURN_TRANS_OUT", "TIMEOUT",
                          f"超时未查到 bank_req_id={self.refund_bank_req_id} 的退款汇入记录")

        return step1_pass and step2_pass
