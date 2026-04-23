import time
import pymysql
from typing import Dict, Any, Optional, List
from utils.es_log_checker import EsLogChecker

try:
    from utils.report_writer import UnifiedReportWriter
except ImportError:
    UnifiedReportWriter = None

_DB_CONFIGS = {
    "sit_in": {
        "host": "devandsitmysql.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "database": "remi-highsun-in",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl": {"verify_cert": False, "check_hostname": False},
    },
}


def _get_in_conn():
    return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **_DB_CONFIGS["sit_in"])


class AgentInSendVerifier:
    def __init__(
        self,
        bank_req_id: str,
        order_id: str,
        amount: float,
        coin_id: str,
        sender_bic: str,
        receiver_bic: str,
        report: Optional[UnifiedReportWriter] = None,
        db_key: str = "sit_in",
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
            es_search_url="",
            kibana_base_url="http://172.188.58.51:5601",
            index_pattern="*",
            cluster_source="sit",
        )

    def get_conn(self):
        if self._conn is None:
            self._conn = _get_in_conn()
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
                "SELECT id, clint_req_id, status, create_time FROM read_record "
                "WHERE clint_req_id = %s ORDER BY create_time DESC LIMIT 1",
                (self.order_id,)
            )
            last_rec = rec
            if rec:
                st = rec.get("status", "")
                if st == "READ":
                    self.log_step(1, "read_record 变为 READ", "PASS", f"status=READ", rec)
                    return {"pass": True, "record": rec}
            time.sleep(self.poll_interval_s)
        self.log_step(1, "read_record 变为 READ", "TIMEOUT",
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
        if status != "OUT_DONE_BACK_ACK":
            self.log_step(2, "COMMON_TRANS_OUT 记录", "FAIL",
                          f"status_flow={status}, expected=OUT_DONE_BACK_ACK", rec)
            return False
        self.log_step(2, "COMMON_TRANS_OUT 记录", "PASS",
                      f"status_flow={status}", rec)
        return True

    def _verify_auto_transfer_inner(self) -> Dict[str, Any]:
        rec = self._db_query(
            "SELECT id, order_id, business_type, status_flow, "
            "sender_remi_id, receive_remi_id, coin_id, amount, direction, ct "
            "FROM message_transaction_record "
            "WHERE business_type = 'AUTO_TRANSFER_INNER' "
            "AND sender_remi_id = %s AND coin_id = %s "
            "AND ABS(amount - %s) < 0.00000001 "
            "ORDER BY ct DESC LIMIT 1",
            (self.sender_bic, self.coin_id, self.amount)
        )
        if not rec:
            self.log_step(3, "AUTO_TRANSFER_INNER 记录(可选)", "PASS",
                          "未找到内部划拨记录（可能余额充足未触发）", None)
            return {"found": False, "record": None}
        status = rec.get("status_flow", "")
        if status != "OUT_DONE_SUCCESS":
            self.log_step(3, "AUTO_TRANSFER_INNER 记录", "FAIL",
                          f"status_flow={status}, expected=OUT_DONE_SUCCESS", rec)
            return {"found": False, "record": rec}
        self.log_step(3, "AUTO_TRANSFER_INNER 记录", "PASS",
                      f"status_flow={status}", rec)
        return {"found": True, "record": rec}

    def _verify_es_update_onchain_status(self) -> bool:
        deadline = time.time() + self.es_timeout_s
        while time.time() < deadline:
            hits, _ = self.es_checker.search_once(
                order_id=self.bank_req_id,
                api_keyword="updateOnchainStatus",
                time_from="now-30m",
                size=5,
            )
            for h in hits:
                msg = str(h.get("message", ""))
                if "ACK" in msg and "COMMON_TRANS_OUT" in msg:
                    self.log_step(4, "ES: updateOnchainStatus (IN 柜面)", "PASS",
                                  f"找到 ACK 回调, pod={h.get('pod_name','')}", h)
                    return True
                if "NAK" in msg:
                    self.log_step(4, "ES: updateOnchainStatus (IN 柜面)", "FAIL",
                                  f"收到 NAK, pod={h.get('pod_name','')}", h)
                    return False
            time.sleep(self.es_interval_s)
        self.log_step(4, "ES: updateOnchainStatus (IN 柜面)", "TIMEOUT",
                      f"超时 {self.es_timeout_s}s 未找到回调日志", None)
        return False

    def _verify_es_inward_message_receive(self, receiver_bic: str) -> bool:
        deadline = time.time() + self.es_timeout_s
        pod_key = receiver_bic[:3].lower()
        while time.time() < deadline:
            hits, _ = self.es_checker.search_once(
                order_id=self.bank_req_id,
                api_keyword="inwardMessageReceive",
                time_from="now-30m",
                size=5,
            )
            pod_hits = [h for h in hits if pod_key in h.get("pod_name", "").lower()]
            if pod_hits:
                hit = pod_hits[0]
                msg = str(hit.get("message", ""))
                if "COMMON_TRANS_IN" in msg:
                    self.log_step(5, f"ES: inwardMessageReceive ({receiver_bic})", "PASS",
                                  f"找到汇入通知, pod={hit.get('pod_name','')}", hit)
                    return True
            time.sleep(self.es_interval_s)
        self.log_step(5, f"ES: inwardMessageReceive ({receiver_bic})", "TIMEOUT",
                      f"超时 {self.es_timeout_s}s 未找到汇入通知日志", None)
        return False

    def verify_all(self, timeout: int = 180) -> bool:
        try:
            self.log_step(0, "IN 发送开始", "RUNNING",
                          f"bank_req_id={self.bank_req_id}, order_id={self.order_id}, "
                          f"receiver={self.receiver_bic}")

            r1 = self._poll_read_record(timeout)
            if not r1["pass"]:
                return False

            if not self._verify_common_trans_out():
                return False

            auto = self._verify_auto_transfer_inner()
            if auto.get("record") and not auto.get("found", True):
                return False

            if not self._verify_es_update_onchain_status():
                return False

            if not self._verify_es_inward_message_receive(self.receiver_bic):
                return False

            self.log_step(6, "IN 发送最终结果", "PASS", "全部核查通过", None)
            return True
        finally:
            self.close_conn()
