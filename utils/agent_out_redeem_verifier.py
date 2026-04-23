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


class AgentOutRedeemVerifier:
    def __init__(
        self,
        amount: float,
        coin_id: str,
        chain_type: str = "solana",
        report: Optional[UnifiedReportWriter] = None,
        db_key: str = "sit_out",
    ):
        self.amount = amount
        self.coin_id = coin_id
        self.chain_type = chain_type
        self.report = report
        self.db_key = db_key
        self.start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.results: List[Dict[str, Any]] = []
        self.es_timeout_s = 120
        self.es_interval_s = 10
        self.redeem_order_id: Optional[str] = None
        self.internal_order_id: Optional[str] = None
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

    def _get_redeem_transfer_out(self, redeem_order_id: str) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT id, order_id, bank_req_id, business_type, status_flow,
                   sender_remi_id, receive_remi_id, coin_id, amount,
                   direction, address, cp_address, tx_id, ct
            FROM message_transaction_record
            WHERE order_id = %s AND business_type = 'REDEEM_TRANSFER_OUT'
            ORDER BY ct DESC LIMIT 1
        """
        return self._db_query(sql, (redeem_order_id,))

    def _get_auto_transfer_inner_by_origin(self, origin_order_id: str) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT r.order_id
            FROM two_step_txn_record r
            WHERE r.origin_order_id = %s AND r.status = 'Success'
            ORDER BY r.create_time DESC LIMIT 1
        """
        two_step = self._db_query(sql, (origin_order_id,))
        if not two_step or not two_step.get("order_id"):
            return None
        self.internal_order_id = two_step["order_id"]

        sql2 = """
            SELECT id, order_id, business_type, status_flow,
                   sender_remi_id, receive_remi_id, coin_id, amount,
                   direction, address, cp_address, ct
            FROM message_transaction_record
            WHERE order_id = %s AND business_type = 'AUTO_TRANSFER_INNER'
            ORDER BY ct DESC LIMIT 1
        """
        return self._db_query(sql2, (self.internal_order_id,))

    def _poll_redeem_transfer_out(self, redeem_order_id: str, timeout_s: int = 120, interval_s: int = 10) -> Dict[str, Any]:
        deadline = time.time() + timeout_s
        last_rec = None
        while time.time() < deadline:
            rec = self._get_redeem_transfer_out(redeem_order_id)
            last_rec = rec
            if rec:
                status = rec.get("status_flow", "")
                if status == "OUT_DONE_BACK_ACK":
                    self.log_step(2, "REDEEM_TRANSFER_OUT 终态", "PASS",
                                  f"status_flow=OUT_DONE_BACK_ACK, order_id={redeem_order_id}", rec)
                    return {"pass": True, "record": rec}
                elif status in ("OUT_SEND_PROCESSING", "PROCESSING"):
                    pass
                else:
                    self.log_step(2, "REDEEM_TRANSFER_OUT 状态", "FAIL",
                                  f"status_flow={status}, expected OUT_DONE_BACK_ACK", rec)
                    return {"pass": False, "record": rec}
            time.sleep(interval_s)

        self.log_step(2, "REDEEM_TRANSFER_OUT 终态", "TIMEOUT",
                      f"超时 {timeout_s}s, last_status={last_rec.get('status_flow') if last_rec else 'N/A'}",
                      last_rec)
        return {"pass": False, "record": last_rec}

    def _verify_auto_transfer_inner(self) -> Dict[str, Any]:
        if not self.internal_order_id:
            self.log_step(3, "AUTO_TRANSFER_INNER 内部划拨", "PASS",
                          "未找到 two_step_txn_record 关联记录（可能无需内部划拨）", None)
            return {"found": False, "record": None}

        rec = self._get_auto_transfer_inner_by_origin(self.redeem_order_id)
        if not rec:
            self.log_step(3, "AUTO_TRANSFER_INNER 内部划拨", "FAIL",
                          f"未找到 order_id={self.internal_order_id}", None)
            return {"found": False, "record": None}

        status = rec.get("status_flow", "")
        if status != "OUT_DONE_SUCCESS":
            self.log_step(3, "AUTO_TRANSFER_INNER 内部划拨", "FAIL",
                          f"status_flow={status}, expected=OUT_DONE_SUCCESS", rec)
            return {"found": False, "record": rec}

        self.log_step(3, "AUTO_TRANSFER_INNER 内部划拨", "PASS",
                      f"status_flow=OUT_DONE_SUCCESS, order_id={self.internal_order_id}", rec)
        return {"found": True, "record": rec}

    def _verify_es_update_onchain_status(self, timeout_s: int = 120, interval_s: int = 10) -> Dict[str, Any]:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            hits, _ = self.es_checker.search_once(
                order_id=self.redeem_order_id,
                api_keyword="UpdateOnchainStatus",
                time_from="now-30m",
                size=5,
            )
            for h in hits:
                msg = str(h.get("message", ""))
                if "REDEEM_TRANSFER_OUT" in msg and self.redeem_order_id in msg:
                    if '"onChainStatus":"ACK"' in msg or '"onChainStatus":"ACK"' in msg.replace(' ', ''):
                        self.log_step(4, "ES: UpdateOnchainStatus (out 柜面)", "PASS",
                                      f"找到 ACK 回调, pod={h.get('pod_name', '')}", h)
                        return {"found": True, "hit": h}
            time.sleep(interval_s)

        self.log_step(4, "ES: UpdateOnchainStatus (out 柜面)", "TIMEOUT",
                      f"超时 {timeout_s}s 未找到 updateOnchainStatus 日志", None)
        return {"found": False}

    def verify_redeem_order(self, redeem_order_id: str, timeout_s: int = 180) -> Dict[str, Any]:
        self.redeem_order_id = redeem_order_id
        try:
            self.log_step(0, "OUT 非实时赎回开始", "RUNNING",
                          f"redeem_order_id={redeem_order_id}, amount={self.amount} {self.coin_id}")

            result2 = self._poll_redeem_transfer_out(redeem_order_id, timeout_s=timeout_s)
            if not result2["pass"]:
                return self._build_result("FAIL")

            auto_result = self._verify_auto_transfer_inner()
            if auto_result.get("record") and not auto_result.get("found", True):
                return self._build_result("FAIL")

            es_result = self._verify_es_update_onchain_status(
                timeout_s=self.es_timeout_s, interval_s=self.es_interval_s)
            if not es_result["found"]:
                return self._build_result("TIMEOUT")

            return self._build_result("PASS")

        finally:
            self.close_conn()

    def _build_result(self, status: str) -> Dict[str, Any]:
        return {
            "flow": "OUT_REDEEM",
            "status": status,
            "redeem_order_id": self.redeem_order_id,
            "internal_order_id": self.internal_order_id,
            "results": self.results,
        }
