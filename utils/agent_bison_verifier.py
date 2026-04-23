import time
import os
from typing import Dict, Any, Optional
from utils.es_log_checker import EsLogChecker
from utils.query_db_data import (
    get_read_record,
    get_common_trans_out,
)

try:
    from utils.report_writer import UnifiedReportWriter
except ImportError:
    UnifiedReportWriter = None


_BISON_DB_CFG = {
    "host": "devandsitmysql.mysql.database.azure.com",
    "user": "dev",
    "password": "Bhy.980226275",
    "database": "remi-bison",
    "port": 3306,
    "charset": "utf8mb4",
    "ssl": {"verify_cert": False, "check_hostname": False},
}


def _get_bison_conn():
    import pymysql
    return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **_BISON_DB_CFG)


class AgentBisonVerifier:
    def __init__(self, db_key: str, bank_req_id: str, order_id: str, amount: float, coin_id: str,
                 report: Optional["UnifiedReportWriter"] = None):
        self.db_key = db_key
        self.bank_req_id = bank_req_id
        self.order_id = order_id
        self.amount = amount
        self.coin_id = coin_id
        self.report = report
        self.start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.results = []
        self.base_timeout_s = int(os.getenv("VERIFY_TIMEOUT_SECONDS", "300"))
        self.poll_interval_s = int(os.getenv("VERIFY_POLL_INTERVAL_SECONDS", "10"))
        self.es_timeout_s = int(os.getenv("VERIFY_ES_TIMEOUT_SECONDS", "120"))
        self.es_interval_s = int(
            os.getenv("VERIFY_ES_POLL_INTERVAL_SECONDS", str(self.poll_interval_s))
        )
        self.es_checker = EsLogChecker(
            es_search_url=os.getenv("ES_SEARCH_URL", ""),
            kibana_base_url=os.getenv("KIBANA_BASE_URL", "http://172.188.58.51:5601"),
            index_pattern=os.getenv("ES_INDEX_PATTERN", "*"),
            cluster_source=os.getenv("ES_CLUSTER_SOURCE", "sit"),
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

    def _poll_read_record(self, timeout_s: int) -> Dict[str, Any]:
        deadline = time.time() + timeout_s
        last_record = None
        while time.time() < deadline:
            record = get_read_record(self.db_key, self.order_id, conn=self.get_conn())
            last_record = record
            if record:
                status = record.get("status")
                if status == "RETURNED_ACK":
                    self.log_step(1, "read_record 状态检查", "PASS",
                                  f"status=RETURNED_ACK", record)
                    return {"pass": True, "record": record}
                elif status == "RETURNED_NAK":
                    fail_reason = record.get("fail_reason") or ""
                    self.log_step(1, "read_record 状态检查", "FAIL",
                                  f"status=RETURNED_NAK, fail_reason={fail_reason}", record)
                    return {"pass": False, "record": record}
            time.sleep(self.poll_interval_s)
        self.log_step(1, "read_record 状态检查", "TIMEOUT",
                      f"超时 {timeout_s}s 未到达 RETURNED_ACK, last_status={last_record.get('status') if last_record else 'N/A'}",
                      last_record)
        return {"pass": False, "record": last_record}

    def _verify_common_trans_out(self) -> bool:
        record = get_common_trans_out(self.db_key, self.bank_req_id, conn=self.get_conn())
        if self.report:
            self.report.add_db_query(self.db_key, "get_common_trans_out",
                                     f"bank_req_id={self.bank_req_id}",
                                     result=record, hit=bool(record))
        if not record:
            self.log_step(2, "COMMON_TRANS_OUT 记录", "FAIL",
                          f"未找到 bank_req_id={self.bank_req_id}", None)
            return False
        status = record.get("status_flow", "")
        if status not in ("OUT_DONE_BACK_ACK", "OUT_DONE_SUCCESS"):
            self.log_step(2, "COMMON_TRANS_OUT 记录", "FAIL",
                          f"status_flow={status}", record)
            return False
        self.log_step(2, "COMMON_TRANS_OUT 记录", "PASS",
                      f"status_flow={status}", record)
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
                                  f"找到 ACK 回调, pod={h.get('pod_name', '')}", h)
                    return True
                if "NAK" in msg:
                    self.log_step(3, "ES updateOnchainStatus", "FAIL",
                                  f"收到 NAK, pod={h.get('pod_name', '')}", h)
                    return False
            time.sleep(self.es_interval_s)
        self.log_step(3, "ES updateOnchainStatus", "TIMEOUT",
                      f"超时 {self.es_timeout_s}s 未找到回调日志", None)
        return False

    def verify_all(self, timeout: Optional[int] = None) -> bool:
        try:
            timeout_s = timeout if timeout is not None else self.base_timeout_s
            print(f"[BISON] 验证参数: timeout={timeout_s}s, poll_interval={self.poll_interval_s}s, "
                  f"es_timeout={self.es_timeout_s}s")

            result = self._poll_read_record(timeout_s)
            if not result["pass"]:
                return False

            if not self._verify_common_trans_out():
                return False

            if not self._verify_es_callback():
                return False

            self.log_step(4, "bison 流程最终结果", "PASS", "全部核查通过", None)
            return True
        finally:
            self.close_conn()
