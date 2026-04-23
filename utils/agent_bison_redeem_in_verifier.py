import time
import pymysql
from typing import Dict, Any, Optional, List
from utils.es_log_checker import EsLogChecker

try:
    from utils.report_writer import UnifiedReportWriter
except ImportError:
    UnifiedReportWriter = None

_DB_CONFIGS = {
    "sit_bison": {
        "host": "devandsitmysql.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "database": "remi-bison",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl": {"verify_cert": False, "check_hostname": False},
    },
}


def _get_bison_conn():
    return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **_DB_CONFIGS["sit_bison"])


class AgentBisonRedeemInVerifier:
    def __init__(
        self,
        burn_tx_id: str,
        amount: float,
        coin_id: str,
        report: Optional[UnifiedReportWriter] = None,
    ):
        self.burn_tx_id = burn_tx_id
        self.amount = amount
        self.coin_id = coin_id
        self.report = report
        self.start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.results: List[Dict[str, Any]] = []
        self.es_timeout_s = 120
        self.es_interval_s = 10
        self.bank_req_id: Optional[str] = None
        self.bison_order_id: Optional[str] = None
        self.es_checker = EsLogChecker(
            es_search_url="",
            kibana_base_url="http://172.188.58.51:5601",
            index_pattern="*",
            cluster_source="sit",
        )
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            self._conn = _get_bison_conn()
        return self._conn

    def close_conn(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def log_step(self, step_no: int, name: str, status: str, detail: str, raw_data: Any = None):
        res = {"step": step_no, "name": name, "status": status, "detail": detail, "raw_data": raw_data}
        self.results.append(res)
        print(f"[Step {step_no}] {name}: {status} - {detail}")
        if self.report:
            self.report.add_step(step_no, name, status, detail, raw_data)

    def _db_query(self, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        conn = self.get_conn()
        cursor = None
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(sql, params)
            return cursor.fetchone()
        finally:
            if cursor:
                cursor.close()

    def _db_query_all(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        conn = self.get_conn()
        cursor = None
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(sql, params)
            return list(cursor.fetchall())
        finally:
            if cursor:
                cursor.close()

    def _find_bank_req_id_from_es(self, timeout_s: int = 120, interval_s: int = 10) -> Dict[str, Any]:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            hits, _ = self.es_checker.search_once(
                order_id=self.burn_tx_id,
                api_keyword="REDEEM_TRANSFER_IN",
                time_from="now-30m",
                size=5,
            )
            for h in hits:
                msg = str(h.get("message", ""))
                if "REDEEM_TRANSFER_IN" in msg and self.burn_tx_id in msg:
                    try:
                        import json
                        import re
                        match = re.search(r'"bank_req_id"\s*:\s*"([^"]+)"', msg)
                        if match:
                            self.bank_req_id = match.group(1)
                        else:
                            match2 = re.search(r'"orderId"\s*:\s*"([^"]+)"', msg)
                            if match2:
                                self.bison_order_id = match2.group(1)
                    except Exception:
                        pass
                    self.log_step(1, "ES: REDEEM_TRANSFER_IN 回调", "PASS",
                                  f"找到 bison 收到赎回通知, tx_id={self.burn_tx_id[:20]}...", h)
                    return {"found": True, "hit": h}
            time.sleep(interval_s)
        self.log_step(1, "ES: REDEEM_TRANSFER_IN 回调", "TIMEOUT",
                      f"超时 {timeout_s}s 未找到 REDEEM_TRANSFER_IN 日志", None)
        return {"found": False}

    def _get_redeem_transfer_in(self) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT id, order_id, bank_req_id, business_type, status_flow,
                   sender_remi_id, receive_remi_id, coin_id, amount,
                   direction, address, tx_id, ct
            FROM message_transaction_record
            WHERE tx_id = %s AND business_type = 'REDEEM_TRANSFER_IN'
            ORDER BY ct DESC LIMIT 1
        """
        return self._db_query(sql, (self.burn_tx_id,))

    def _get_redeem_external_transfer(self) -> Optional[Dict[str, Any]]:
        if not self.bank_req_id and not self.bison_order_id:
            return None
        if self.bank_req_id:
            sql = """
                SELECT id, order_id, business_type, status_flow,
                       sender_remi_id, receive_remi_id, coin_id, amount,
                       direction, address, ct
                FROM message_transaction_record
                WHERE bank_req_id = %s AND business_type = 'REDEEM_EXTERNAL_TRANSFER'
                ORDER BY ct DESC LIMIT 1
            """
            return self._db_query(sql, (self.bank_req_id,))
        else:
            sql2 = """
                SELECT id, order_id, business_type, status_flow,
                       sender_remi_id, receive_remi_id, coin_id, amount,
                       direction, address, ct
                FROM message_transaction_record
                WHERE order_id LIKE %s AND business_type = 'REDEEM_EXTERNAL_TRANSFER'
                ORDER BY ct DESC LIMIT 1
            """
            like_pattern = self.bison_order_id + "_EXTERNAL"
            return self._db_query(sql2, (like_pattern,))

    def _get_info_trans_out_redeem_resp(self) -> Optional[Dict[str, Any]]:
        if not self.bank_req_id and not self.bison_order_id:
            return None
        if self.bank_req_id:
            sql = """
                SELECT id, order_id, business_type, status_flow,
                       sender_remi_id, receive_remi_id, coin_id, amount,
                       direction, address, memo, ct
                FROM message_transaction_record
                WHERE bank_req_id = %s AND business_type = 'INFO_TRANS_OUT'
                ORDER BY ct DESC LIMIT 1
            """
            return self._db_query(sql, (self.bank_req_id,))
        else:
            sql2 = """
                SELECT id, order_id, business_type, status_flow,
                       sender_remi_id, receive_remi_id, coin_id, amount,
                       direction, address, memo, ct
                FROM message_transaction_record
                WHERE order_id LIKE %s AND business_type = 'INFO_TRANS_OUT'
                ORDER BY ct DESC LIMIT 1
            """
            like_pattern = self.bison_order_id + "_REDEEM_RESP"
            return self._db_query(sql2, (like_pattern,))

    def _verify_redeem_transfer_in(self) -> bool:
        rec = self._get_redeem_transfer_in()
        if not rec:
            self.log_step(2, "REDEEM_TRANSFER_IN 记录", "FAIL",
                          f"未找到 tx_id={self.burn_tx_id} 的 REDEEM_TRANSFER_IN", None)
            return False
        status = rec.get("status_flow", "")
        if status != "IN_DONE":
            self.log_step(2, "REDEEM_TRANSFER_IN 记录", "FAIL",
                          f"status_flow={status}, expected=IN_DONE", rec)
            return False
        self.bison_order_id = rec.get("order_id")
        self.log_step(2, "REDEEM_TRANSFER_IN 记录", "PASS",
                      f"status_flow=IN_DONE, order_id={self.bison_order_id}", rec)
        return True

    def _verify_redeem_external_transfer(self) -> bool:
        rec = self._get_redeem_external_transfer()
        if not rec:
            self.log_step(3, "REDEEM_EXTERNAL_TRANSFER 记录", "FAIL",
                          f"未找到外部划转记录", None)
            return False
        status = rec.get("status_flow", "")
        if status != "OUT_DONE_SUCCESS":
            self.log_step(3, "REDEEM_EXTERNAL_TRANSFER 记录", "FAIL",
                          f"status_flow={status}, expected=OUT_DONE_SUCCESS", rec)
            return False
        self.log_step(3, "REDEEM_EXTERNAL_TRANSFER 记录", "PASS",
                      f"status_flow=OUT_DONE_SUCCESS, order_id={rec.get('order_id')}", rec)
        return True

    def _verify_info_trans_out_redeem_resp(self) -> bool:
        rec = self._get_info_trans_out_redeem_resp()
        if not rec:
            self.log_step(4, "INFO_TRANS_OUT (REDEEM_RESP) 记录", "FAIL",
                          f"未找到信息币回复记录", None)
            return False
        status = rec.get("status_flow", "")
        if status != "OUT_DONE_SUCCESS":
            self.log_step(4, "INFO_TRANS_OUT (REDEEM_RESP) 记录", "FAIL",
                          f"status_flow={status}, expected=OUT_DONE_SUCCESS", rec)
            return False
        memo = rec.get("memo") or ""
        if "REDEEM_RESP" not in memo and "COMPLETED" not in memo:
            self.log_step(4, "INFO_TRANS_OUT (REDEEM_RESP) memo 内容", "WARN",
                          f"memo 可能不含 COMPLETED: {memo[:100]}", rec)
        else:
            self.log_step(4, "INFO_TRANS_OUT (REDEEM_RESP) 记录", "PASS",
                          f"status_flow=OUT_DONE_SUCCESS, memo contains COMPLETED", rec)
        return True

    def verify_all(self, timeout: int = 180) -> Dict[str, Any]:
        try:
            self.log_step(0, "BISON 收到实时赎回外转开始", "RUNNING",
                          f"burn_tx_id={self.burn_tx_id}, amount={self.amount} {self.coin_id}")

            es_result = self._find_bank_req_id_from_es(
                timeout_s=min(timeout, self.es_timeout_s),
                interval_s=self.es_interval_s,
            )
            if not es_result["found"]:
                return self._build_result("TIMEOUT")

            if not self._verify_redeem_transfer_in():
                return self._build_result("FAIL")

            if not self._verify_redeem_external_transfer():
                return self._build_result("FAIL")

            if not self._verify_info_trans_out_redeem_resp():
                return self._build_result("FAIL")

            return self._build_result("PASS")

        finally:
            self.close_conn()

    def _build_result(self, status: str) -> Dict[str, Any]:
        return {
            "flow": "BISON_REDEEM_IN",
            "status": status,
            "burn_tx_id": self.burn_tx_id,
            "bank_req_id": self.bank_req_id,
            "bison_order_id": self.bison_order_id,
            "results": self.results,
        }
