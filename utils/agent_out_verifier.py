import time
import pymysql
from typing import Dict, Any, Optional, List
from utils.es_log_checker import EsLogChecker

try:
    from utils.report_writer import UnifiedReportWriter
except ImportError:
    UnifiedReportWriter = None

_DB_CONFIGS = {
    "sit_out": {
        "host": "devandsitmysql.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "database": "remi-highsun-out",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl": {"verify_cert": False, "check_hostname": False},
    },
}


def _get_out_conn():
    return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **_DB_CONFIGS["sit_out"])


class AgentOutVerifier:
    def __init__(
        self,
        bank_req_id: str,
        order_id: str,
        amount: float,
        coin_id: str,
        sender_bic: str,
        receiver_bic: str,
        report: Optional[UnifiedReportWriter] = None,
        db_key: str = "sit_out",
    ):
        self.bank_req_id = bank_req_id
        self.order_id = order_id
        self.amount = amount
        self.coin_id = coin_id
        self.sender_bic = sender_bic
        self.receiver_bic = receiver_bic
        self.report = report
        self.db_key = db_key
        self.start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.results: List[Dict[str, Any]] = []
        self.es_timeout_s = 120
        self.es_interval_s = 10
        self.es_checker = EsLogChecker(
            es_search_url="",
            kibana_base_url="http://172.188.58.51:5601",
            index_pattern="*",
            cluster_source="sit",
        )
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            self._conn = _get_out_conn()
        return self._conn

    def close_conn(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def log_step(self, step_no: int, name: str, status: str, detail: str, raw_data: Any = None):
        res = {
            "step": step_no,
            "name": name,
            "status": status,
            "detail": detail,
            "raw_data": raw_data,
        }
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

    def _get_read_record(self) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT id, clint_req_id, status, create_time, update_time
            FROM read_record
            WHERE clint_req_id = %s
            ORDER BY create_time DESC
            LIMIT 1
        """
        return self._db_query(sql, (self.order_id,))

    def _get_message_transaction_common_out(self) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT id, order_id, bank_req_id, business_type, status_flow,
                   sender_remi_id, receive_remi_id, coin_id, amount,
                   direction, address, tx_id, ct
            FROM message_transaction_record
            WHERE bank_req_id = %s AND business_type = 'COMMON_TRANS_OUT'
            ORDER BY ct DESC
            LIMIT 1
        """
        return self._db_query(sql, (self.bank_req_id,))

    def _get_message_transaction_auto_transfer_inner(self) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT id, order_id, business_type, status_flow,
                   sender_remi_id, receive_remi_id, coin_id, amount,
                   direction, address, ct
            FROM message_transaction_record
            WHERE business_type = 'AUTO_TRANSFER_INNER'
              AND sender_remi_id = %s
              AND coin_id = %s
              AND ABS(amount - %s) < 0.00000001
            ORDER BY ct DESC
            LIMIT 1
        """
        return self._db_query(sql, (self.sender_bic, self.coin_id, self.amount))

    def _poll_read_record(self, timeout_s: int = 120, interval_s: int = 5) -> Dict[str, Any]:
        deadline = time.time() + timeout_s
        attempt = 0
        last_rec = None
        while time.time() < deadline:
            attempt += 1
            rec = self._get_read_record()
            last_rec = rec
            if rec and rec.get("status") == "READ":
                self.log_step(1, "read_record 变为 READ", "PASS",
                              f"read_record READ, id={rec.get('id')}", rec)
                return {"found": True, "record": rec}
            time.sleep(interval_s)
        return {"found": False, "record": last_rec}

    def _verify_common_trans_out(self) -> Dict[str, Any]:
        rec = self._get_message_transaction_common_out()
        if not rec:
            self.log_step(2, "COMMON_TRANS_OUT 记录存在", "FAIL",
                          f"未找到 bank_req_id={self.bank_req_id} 的 COMMON_TRANS_OUT", None)
            return {"pass": False, "record": None}

        status = rec.get("status_flow", "")
        if status != "OUT_DONE_BACK_ACK":
            self.log_step(2, "COMMON_TRANS_OUT status_flow", "FAIL",
                          f"status_flow={status}, expected=OUT_DONE_BACK_ACK", rec)
            return {"pass": False, "record": rec}

        if abs(float(rec.get("amount", 0)) - self.amount) > 0.00000001:
            self.log_step(2, "COMMON_TRANS_OUT amount", "FAIL",
                          f"amount={rec.get('amount')}, expected={self.amount}", rec)
            return {"pass": False, "record": rec}

        if rec.get("coin_id") != self.coin_id:
            self.log_step(2, "COMMON_TRANS_OUT coin_id", "FAIL",
                          f"coin_id={rec.get('coin_id')}, expected={self.coin_id}", rec)
            return {"pass": False, "record": rec}

        self.log_step(2, "COMMON_TRANS_OUT 记录验证", "PASS",
                      f"status_flow=OUT_DONE_BACK_ACK, amount={rec.get('amount')}, "
                      f"coin_id={rec.get('coin_id')}, tx_id={str(rec.get('tx_id') or '')[:20]}...",
                      rec)
        return {"pass": True, "record": rec}

    def _verify_auto_transfer_inner(self) -> Dict[str, Any]:
        rec = self._get_message_transaction_auto_transfer_inner()
        if not rec:
            self.log_step(3, "AUTO_TRANSFER_INNER 记录(可选)", "PASS",
                          "未找到内部划拨记录（可能余额充足未触发）", None)
            return {"found": False, "record": None}

        status = rec.get("status_flow", "")
        if status != "OUT_DONE_SUCCESS":
            self.log_step(3, "AUTO_TRANSFER_INNER status_flow", "FAIL",
                          f"status_flow={status}, expected=OUT_DONE_SUCCESS", rec)
            return {"pass": False, "record": rec}

        self.log_step(3, "AUTO_TRANSFER_INNER 记录验证", "PASS",
                      f"status_flow=OUT_DONE_SUCCESS, order_id={rec.get('order_id')}", rec)
        return {"found": True, "record": rec}

    def _verify_es_update_onchain_status(self, timeout_s: int = 120, interval_s: int = 10) -> Dict[str, Any]:
        deadline = time.time() + timeout_s
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            hits, _ = self.es_checker.search_once(
                order_id=self.bank_req_id,
                api_keyword="updateOnchainStatus",
                time_from="now-30m",
                size=5,
            )
            pod_hits = [h for h in hits if "out" in h.get("pod_name", "").lower()]
            if pod_hits:
                hit = pod_hits[0]
                msg = hit.get("message", "")
                if "ACK" in msg and "COMMON_TRANS_OUT" in msg:
                    self.log_step(4, "ES: updateOnchainStatus (out 柜面)", "PASS",
                                   f"找到 ACK 回调, pod={hit.get('pod_name', '')}", hit)
                    return {"found": True, "hit": hit}
                elif "NAK" in msg:
                    self.log_step(4, "ES: updateOnchainStatus (out 柜面)", "FAIL",
                                   f"收到 NAK, pod={hit.get('pod_name', '')}", hit)
                    return {"found": True, "hit": hit, "nak": True}
            time.sleep(interval_s)

        self.log_step(4, "ES: updateOnchainStatus (out 柜面)", "TIMEOUT",
                      f"超时 {timeout_s}s 未找到 updateOnchainStatus 日志", None)
        return {"found": False}

    def _verify_es_inward_message_receive_fxb(self, timeout_s: int = 120, interval_s: int = 10) -> Dict[str, Any]:
        deadline = time.time() + timeout_s
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            hits, _ = self.es_checker.search_once(
                order_id=self.bank_req_id,
                api_keyword="inwardMessageReceive",
                time_from="now-30m",
                size=5,
            )
            pod_hits = [h for h in hits if "fxb" in h.get("pod_name", "").lower()]
            if pod_hits:
                hit = pod_hits[0]
                msg = hit.get("message", "")
                if "COMMON_TRANS_IN" in msg:
                    self.log_step(5, "ES: inwardMessageReceive (FXB 柜面)", "PASS",
                                   f"找到 FXB 汇入通知, pod={hit.get('pod_name', '')}", hit)
                    return {"found": True, "hit": hit}
            time.sleep(interval_s)

        self.log_step(5, "ES: inwardMessageReceive (FXB 柜面)", "TIMEOUT",
                      f"超时 {timeout_s}s 未找到 FXB inwardMessageReceive 日志", None)
        return {"found": False}

    def verify_all(self, timeout: int = 180) -> Dict[str, Any]:
        try:
            self.log_step(0, "OUT 流程开始", "RUNNING",
                          f"bank_req_id={self.bank_req_id}, order_id={self.order_id}, "
                          f"amount={self.amount} {self.coin_id}")

            poll_result = self._poll_read_record(timeout_s=timeout, interval_s=5)
            if not poll_result["found"]:
                self.log_step(1, "read_record 变为 READ", "TIMEOUT",
                              f"超时未变为 READ, last_status={poll_result.get('record', {}).get('status') if poll_result.get('record') else 'N/A'}", None)
                return self._build_result("TIMEOUT")

            out_result = self._verify_common_trans_out()
            if not out_result["pass"]:
                return self._build_result("FAIL")

            auto_result = self._verify_auto_transfer_inner()
            if auto_result.get("found") and not auto_result.get("pass"):
                return self._build_result("FAIL")

            es1_result = self._verify_es_update_onchain_status(
                timeout_s=self.es_timeout_s, interval_s=self.es_interval_s)
            if not es1_result["found"]:
                return self._build_result("TIMEOUT")
            if es1_result.get("nak"):
                return self._build_result("FAIL")

            es2_result = self._verify_es_inward_message_receive_fxb(
                timeout_s=self.es_timeout_s, interval_s=self.es_interval_s)
            if not es2_result["found"]:
                return self._build_result("TIMEOUT")

            return self._build_result("PASS")

        finally:
            self.close_conn()

    def _build_result(self, status: str) -> Dict[str, Any]:
        return {
            "flow": "out",
            "status": status,
            "bank_req_id": self.bank_req_id,
            "order_id": self.order_id,
            "results": self.results,
        }
