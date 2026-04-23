import time
import json
import os
import pymysql
from typing import Dict, Any, Optional
from utils.es_log_checker import EsLogChecker

try:
    from utils.report_writer import UnifiedReportWriter
except ImportError:
    UnifiedReportWriter = None

_DB_CONFIGS = {
    "sit_fxc": {
        "host": "devandsitmysql.mysql.database.azure.com",
        "user": "dev",
        "password": "Bhy.980226275",
        "database": "remi-fxc",
        "port": 3306,
        "charset": "utf8mb4",
        "ssl": {"verify_cert": False, "check_hostname": False},
    },
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


def _get_conn(db_key: str) -> pymysql.Connection:
    cfg = _DB_CONFIGS.get(db_key)
    if not cfg:
        raise ValueError(f"Unknown db_key: {db_key}")
    return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **cfg)


class AgentBaseVerifier:
    def __init__(
        self,
        db_key: str,
        report: Optional[UnifiedReportWriter] = None,
        es_timeout_s: int = 120,
        es_interval_s: int = 10,
    ):
        self.db_key = db_key
        self.report = report
        self.es_timeout_s = es_timeout_s
        self.es_interval_s = es_interval_s
        self.es_checker = EsLogChecker(
            es_search_url=os.getenv("ES_SEARCH_URL", ""),
            kibana_base_url=os.getenv("KIBANA_BASE_URL", "http://172.188.58.51:5601"),
            index_pattern=os.getenv("ES_INDEX_PATTERN", "*"),
            cluster_source=os.getenv("ES_CLUSTER_SOURCE", "sit"),
        )
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            self._conn = _get_conn(self.db_key)
        return self._conn

    def close_conn(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def log_step(self, step: int, name: str, status: str, detail: str,
                 raw_data: Optional[Dict[str, Any]] = None):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        entry = {
            "step": step, "name": name, "status": status,
            "detail": detail, "timestamp": ts,
            "raw_data": raw_data or {},
        }
        print(f"[Step {step}] {name}: {status} - {detail}")
        if self.report:
            self.report.step_results.append(entry)

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

    def _db_query_all(self, sql: str, params: tuple = ()) -> list:
        conn = self.get_conn()
        cursor = None
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(sql, params)
            return cursor.fetchall()
        finally:
            if cursor:
                cursor.close()

    def _check_es_log(self, keyword: str, order_id: str, deadline: float) -> bool:
        hit_count = 0
        while time.time() < deadline:
            hits, _ = self.es_checker.search_once(
                order_id=order_id,
                api_keyword=keyword,
                time_from="now-30m",
                size=10,
            )
            if hits:
                hit_count = len(hits)
                break
            time.sleep(self.es_interval_s)
        if self.report:
            self.report.es_queries.append({
                "order_id": order_id,
                "keyword": keyword,
                "hit_count": hit_count,
                "source": "kibana_proxy",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        return hit_count > 0
