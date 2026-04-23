import time
import os
import pymysql
from typing import Dict, Any, Optional, List
from utils.es_log_checker import EsLogChecker

try:
    from utils.report_writer import UnifiedReportWriter
except ImportError:
    UnifiedReportWriter = None

_DB_CONFIGS = {
    "sit_fxb": {
        "host": "devandsitmysql.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "database": "remi-fxb",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl": {"verify_cert": False, "check_hostname": False},
    },
}


def _get_fxb_conn():
    return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **_DB_CONFIGS["sit_fxb"])


class AgentFxbSendVerifier:
    def __init__(
        self,
        bank_req_id: str,
        order_id: str,
        amount: float,
        coin_id: str,
        sender_bic: str,
        receiver_bic: str,
        report: Optional[UnifiedReportWriter] = None,
        db_key: str = "sit_fxb",
    ):
        self.bank_req_id = bank_req_id
        self.order_id = order_id
        self.amount = amount
        self.coin_id = coin_id
        self.sender_bic = sender_bic
        self.receiver_bic = receiver_bic
        self.report = report
        self.db_key = db_key
        self.results: List[Dict[str, Any]] = []
        self.poll_interval_s = 10
        self.es_timeout_s = 120
        self.es_interval_s = 10
        self._conn = None
        self.es_checker = EsLogChecker(
            es_search_url=os.getenv("ES_SEARCH_URL", ""),
            kibana_base_url=os.getenv("KIBANA_BASE_URL", "http://172.188.58.51:5601"),
            index_pattern=os.getenv("ES_INDEX_PATTERN", "*"),
            cluster_source=os.getenv("ES_CLUSTER_SOURCE", "sit"),
        )

    def get_conn(self):
        if self._conn is None:
            self._conn = _get_fxb_conn()
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

    def _poll_read_record(self, timeout_s: int) -> Dict[str, Any]:
        deadline = time.time() + timeout_s
        last_rec = None
        while time.time() < deadline:
            rec = self._db_query(
                "SELECT id, service_uni_id, status, txt_info FROM read_record WHERE service_uni_id = %s LIMIT 1",
                (self.bank_req_id,)
            )
            last_rec = rec
            if rec:
                st = rec.get("status", "")
                if st == "RETURNED_ACK":
                    self.log_step(1, "read_record 状态检查", "PASS", f"status=RETURNED_ACK", rec)
                    return {"pass": True, "record": rec}
                elif st == "RETURNED_NAK":
                    self.log_step(1, "read_record 状态检查", "FAIL",
                                  f"status=RETURNED_NAK, txt_info={rec.get('txt_info','')}", rec)
                    return {"pass": False, "record": rec}
            time.sleep(self.poll_interval_s)
        self.log_step(1, "read_record 状态检查", "TIMEOUT",
                      f"超时 {timeout_s}s, last_status={last_rec.get('status') if last_rec else 'N/A'}",
                      last_rec)
        return {"pass": False, "record": last_rec}

    def _verify_common_trans_out(self) -> bool:
        rec = self._db_query(
            "SELECT id, order_id, bank_req_id, business_type, status_flow, "
            "sender_remi_id, receive_remi_id, coin_id, amount, direction, tx_id, ct "
            "FROM message_transaction_record "
            "WHERE bank_req_id = %s AND business_type = 'COMMON_TRANS_OUT' ORDER BY ct DESC LIMIT 1",
            (self.bank_req_id,)
        )
        if self.report:
            self.report.add_db_query(self.db_key, "COMMON_TRANS_OUT",
                                     f"bank_req_id={self.bank_req_id}",
                                     result=rec, hit=bool(rec))
        if not rec:
            self.log_step(2, "COMMON_TRANS_OUT 记录", "FAIL",
                          f"未找到 bank_req_id={self.bank_req_id}", None)
            return False
        status = rec.get("status_flow", "")
        if status not in ("OUT_DONE_BACK_ACK", "OUT_DONE_SUCCESS"):
            self.log_step(2, "COMMON_TRANS_OUT 记录", "FAIL",
                          f"status_flow={status}", rec)
            return False
        self.log_step(2, "COMMON_TRANS_OUT 记录", "PASS",
                      f"status_flow={status}", rec)
        return True

    def _verify_es_callback(self) -> bool:
        deadline = time.time() + self.es_timeout_s
        while time.time() < deadline:
            hits, _ = self.es_checker.search_once(
                order_id=self.order_id,
                api_keyword="updateOnchainStatus",
                time_from="now-30m",
                size=10,
            )
            for h in hits:
                msg = str(h.get("message", ""))
                if "ACK" in msg and "COMMON_TRANS_OUT" in msg:
                    self.log_step(3, "ES updateOnchainStatus", "PASS",
                                  f"找到 ACK 回调, pod={h.get('pod_name','')}", h)
                    return True
                if "NAK" in msg:
                    self.log_step(3, "ES updateOnchainStatus", "FAIL",
                                  f"收到 NAK, pod={h.get('pod_name','')}", h)
                    return False
            time.sleep(self.es_interval_s)
        self.log_step(3, "ES updateOnchainStatus", "TIMEOUT",
                      f"超时 {self.es_timeout_s}s 未找到回调日志", None)
        return False

    def verify_all(self, timeout: int = 180) -> bool:
        try:
            self.log_step(0, "FXB 发送开始", "RUNNING",
                          f"bank_req_id={self.bank_req_id}, order_id={self.order_id}, "
                          f"amount={self.amount} {self.coin_id}, receiver={self.receiver_bic}")

            r1 = self._poll_read_record(timeout)
            if not r1["pass"]:
                return False

            if not self._verify_common_trans_out():
                return False

            if not self._verify_es_callback():
                return False

            self.log_step(4, "FXB 发送最终结果", "PASS", "全部核查通过", None)
            return True
        finally:
            self.close_conn()
