import time
import json
import os
import re
from typing import Dict, List, Any, Optional

def get_db_conn(db_type: str):
    import pymysql
    cfg = {
        "sit_fxa": {"host": "172.188.58.51", "port": 3306, "user": "root",
                    "password": "root123", "database": "remi-fxa", "charset": "utf8mb4"},
        "sit_fxb": {"host": "devandsitmysql.mysql.database.azure.com", "port": 3306,
                    "user": "dev", "password": "Bhy.980226275",
                    "database": "remi-fxb", "charset": "utf8mb4",
                    "ssl": {"verify_cert": False, "check_hostname": False}},
    }
    conf = cfg.get(db_type, cfg["sit_fxa"])
    conn = pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **conf)
    return conn



from utils.es_log_checker import EsLogChecker

try:
    from utils.report_writer import UnifiedReportWriter
except ImportError:
    UnifiedReportWriter = None

_DB_DEBUG_LAST_PRINT_AT = {}
_DB_DEBUG_INTERVAL_SECONDS = 30
_DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "summary").strip().lower()


def _db_debug(db_type, action, sql, params=None, result=None):
    key = (db_type, action, sql, str(params))
    now = time.time()
    last = _DB_DEBUG_LAST_PRINT_AT.get(key, 0)
    if now - last < _DB_DEBUG_INTERVAL_SECONDS:
        return
    _DB_DEBUG_LAST_PRINT_AT[key] = now
    if _DEBUG_LEVEL != "detail":
        if result is not None:
            if isinstance(result, list):
                first = result[0]
                status = first.get("status_flow") or first.get("status") or f"OK({len(result)} rows)"
            else:
                status = result.get("status_flow") or result.get("status") or "OK"
            print(f"[DB][{db_type}] {action} | status={status}")
        else:
            print(f"[DB][{db_type}] {action} | no_record")
        return
    print(f"[DB][{db_type}] {action}")
    print(f"[DB][{db_type}] SQL: {sql}")
    if params is not None:
        print(f"[DB][{db_type}] PARAMS: {params}")
    if result is not None:
        try:
            print(f"[DB][{db_type}] RESULT: {str(result)}")
        except Exception:
            print(f"[DB][{db_type}] RESULT: {result}")


def get_common_trans_in(db_type: str, bank_req_id: str):
    conn = get_db_conn(db_type)
    try:
        with conn.cursor() as cur:
            sql = (
                f"SELECT * FROM message_transaction_record "
                f"WHERE bank_req_id=%s AND business_type='COMMON_TRANS_IN' "
                f"ORDER BY id DESC LIMIT 1"
            )
            params = (bank_req_id,)
            _db_debug(db_type, "QUERY get_common_trans_in", sql, params=params)
            cur.execute(sql, params)
            row = cur.fetchone()
            _db_debug(db_type, "RESULT get_common_trans_in", sql, params=params, result=row)
            return row
    finally:
        conn.close()


def get_auto_transfer_inner_by_order_prefix(db_type: str, order_prefix: str):
    conn = get_db_conn(db_type)
    try:
        with conn.cursor() as cur:
            sql = (
                f"SELECT * FROM message_transaction_record "
                f"WHERE business_type='AUTO_TRANSFER_INNER' AND order_id=%s "
                f"ORDER BY id DESC LIMIT 1"
            )
            params = (f"{order_prefix}_INTERNAL",)
            _db_debug(db_type, "QUERY get_auto_transfer_inner_by_order_prefix", sql, tuple(params))
            cur.execute(sql, params)
            row = cur.fetchone()
            _db_debug(db_type, "RESULT get_auto_transfer_inner_by_order_prefix", sql, tuple(params), result=row)
            return row
    finally:
        conn.close()


def get_redeem_trans_out_by_order_prefix(db_type: str, order_prefix: str):
    conn = get_db_conn(db_type)
    try:
        with conn.cursor() as cur:
            sql = (
                f"SELECT * FROM message_transaction_record "
                f"WHERE business_type='REDEEM_TRANS_OUT' AND order_id=%s "
                f"ORDER BY id DESC LIMIT 1"
            )
            params = (f"{order_prefix}_BURN",)
            _db_debug(db_type, "QUERY get_redeem_trans_out_by_order_prefix", sql, params)
            cur.execute(sql, params)
            row = cur.fetchone()
            _db_debug(db_type, "RESULT get_redeem_trans_out_by_order_prefix", sql, params, result=row)
            return row
    finally:
        conn.close()


def get_info_trans_in_candidates(db_type: str, min_ct: Optional[str] = None, limit: int = 30):
    conn = get_db_conn(db_type)
    try:
        with conn.cursor() as cur:
            sql = (
                "SELECT * FROM message_transaction_record "
                "WHERE business_type='INFO_TRANS_IN' "
                "AND order_id LIKE 'IN_%%' "
                "AND memo LIKE '%%REDEEM_RESP%%' "
            )
            params: List[Any] = []
            if min_ct:
                sql += "AND ct >= %s "
                params.append(min_ct)
            sql += "ORDER BY id DESC LIMIT %s"
            params.append(limit)
            _db_debug(db_type, "QUERY get_info_trans_in_candidates", sql, params)
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            _db_debug(db_type, "RESULT get_info_trans_in_candidates", sql, params, result=rows)
            return rows
    finally:
        conn.close()


class AgentFxbVerifier:
    """
    Agent FXB（不持币成员行）汇入自动赎回验证器。

    5 步人工检查流程：
    Step 1: ES 查到 inwardMessagePostToBank 回调
    Step 2: COMMON_TRANS_IN，终态 IN_DONE，tx_id = ES 回调里的 txId
    Step 3: AUTO_TRANSFER_INNER（Receive -> Redeem），终态 OUT_DONE_SUCCESS
    Step 4: REDEEM_TRANS_OUT（Redeem -> BISON Burn），终态 OUT_DONE_SUCCESS
    Step 5: INFO_TRANS_IN（REDEEM_RESP），终态 INFO_IN，status=COMPLETED
    """

    PROCESSING_KEYS = (
        "PROCESSING", "PENDING", "READ", "INIT",
        "OUT_SEND", "IN_RESULT", "SEND", "RECEIVE",
    )

    def __init__(self, db_key: str, bank_req_id: str, amount: float, coin_id: str,
                 report: Optional["UnifiedReportWriter"] = None):
        self.db_key = db_key
        self.bank_req_id = bank_req_id
        self.amount = amount
        self.coin_id = coin_id
        self.report = report
        self.start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.results = []
        self.es_checker = EsLogChecker(
            es_search_url=os.getenv("ES_SEARCH_URL", ""),
            kibana_base_url=os.getenv("KIBANA_BASE_URL", "http://172.188.58.51:5601"),
            index_pattern=os.getenv("ES_INDEX_PATTERN", "*"),
            cluster_source=os.getenv("ES_CLUSTER_SOURCE", "sit"),
        )

    def log_step(self, step_no: int, name: str, status: str, detail: str, raw_data: Any = None):
        res = {
            "step": step_no, "name": name, "status": status,
            "detail": detail, "raw_data": raw_data,
        }
        self.results.append(res)
        print(f"[Step {step_no}] {name}: {status} - {detail}")
        if self.report:
            self.report.add_step(step_no, name, status, detail, raw_data)

    def _es_search(self, keyword: str, bank_req_id: str, time_from: str = None, size: int = 200) -> list:
        if time_from is None:
            time_from = os.getenv("VERIFY_ES_TIME_RANGE", "now-12h")
        hits, source = self.es_checker.search_once(
            order_id=bank_req_id,
            api_keyword=keyword,
            time_from=time_from,
            size=size,
            timeout_s=20,
        )
        if self.report:
            self.report.add_es_query(source or "unknown", bank_req_id, keyword,
                                     time_from, len(hits),
                                     f"source={source}, status={'PASS' if hits else 'no_hit'}")
        return hits

    def _extract_tx_id_from_es_log(self, hits: list, keyword: str, bank_req_id: str) -> Optional[str]:
        for h in hits:
            msg = h.get("_source", {}).get("message", "")
            if keyword.lower() not in msg.lower():
                continue
            if bank_req_id not in msg:
                continue
            m = re.search(r'txId:([^\s,}]+)', msg)
            if m:
                return m.group(1)
        return None

    def _has_processing(self, snapshot: Dict[str, Any]) -> bool:
        for v in snapshot.values():
            if not isinstance(v, str):
                continue
            vu = v.upper()
            if any(k.upper() in vu for k in self.PROCESSING_KEYS):
                return True
        return False

    def verify_all(self, timeout: int = 600, interval: int = 10):
        deadline = time.time() + timeout
        print(
            f"[FXB] 汇入自动赎回验证启动 | bank_req_id={self.bank_req_id}, "
            f"timeout={timeout}s, interval={interval}s"
        )

        # Step 1: ES 回调
        print("[Step 1] 开始 ES 回调日志检查...")
        es_step_passed = False
        es_tx_id = None
        while time.time() < deadline:
            hits = self._es_search("inwardMessagePostToBank", self.bank_req_id)
            if hits:
                es_tx_id = self._extract_tx_id_from_es_log(hits, "inwardMessagePostToBank", self.bank_req_id)
                if es_tx_id:
                    self.log_step(1, "ES inwardMessagePostToBank 回调", "PASS",
                                  f"已查到回调日志，txId={es_tx_id}", {"tx_id": es_tx_id, "hit_count": len(hits)})
                    es_step_passed = True
                    break
            time.sleep(interval)

        if not es_step_passed:
            self.log_step(1, "ES inwardMessagePostToBank 回调", "FAIL",
                          f"未查到回调日志，请检查 bank_req_id={self.bank_req_id} 是否正确")
            return False

        # Step 2: COMMON_TRANS_IN
        print(f"[Step 2] 开始检查 COMMON_TRANS_IN, bank_req_id={self.bank_req_id}...")
        common_in = None
        while time.time() < deadline:
            common_in = get_common_trans_in(self.db_key, self.bank_req_id)
            if self.report:
                self.report.add_db_query(self.db_key, "get_common_trans_in",
                                         f"bank_req_id={self.bank_req_id}", result=common_in, hit=bool(common_in))
            if common_in and common_in.get("status_flow") == "IN_DONE":
                recorded_tx_id = common_in.get("tx_id", "")
                if recorded_tx_id and es_tx_id and recorded_tx_id != es_tx_id:
                    self.log_step(2, "COMMON_TRANS_IN 终态检查", "FAIL",
                                  f"tx_id 不匹配：DB记录={recorded_tx_id}, ES回调={es_tx_id}", common_in)
                    return False
                self.log_step(2, "COMMON_TRANS_IN 终态检查", "PASS",
                              f"状态为 IN_DONE，tx_id={recorded_tx_id}", common_in)
                break
            time.sleep(interval)

        if not common_in:
            self.log_step(2, "COMMON_TRANS_IN 终态检查", "FAIL", "未找到记录")
            return False

        order_prefix = common_in.get("order_id", "")
        if not order_prefix:
            self.log_step(2, "COMMON_TRANS_IN 终态检查", "FAIL", "order_id 为空，无法继续后续步骤")
            return False

        # Step 3: AUTO_TRANSFER_INNER（Receive -> Redeem）
        internal_order = f"{order_prefix}_INTERNAL"
        print(f"[Step 3] 检查 AUTO_TRANSFER_INNER (Receive->Redeem), order_id={internal_order}...")
        actual_amount = float(common_in.get("amount", 0))
        auto1 = None
        while time.time() < deadline:
            auto1 = get_auto_transfer_inner_by_order_prefix(self.db_key, order_prefix)
            if self.report:
                self.report.add_db_query(self.db_key, "get_auto_transfer_inner_by_order_prefix",
                                         f"order_prefix={order_prefix}", result=auto1, hit=bool(auto1))
            if auto1 and auto1.get("status_flow") == "OUT_DONE_SUCCESS":
                if float(auto1.get("amount", 0)) != actual_amount:
                    self.log_step(3, "AUTO_TRANSFER_INNER (Receive->Redeem)", "FAIL",
                                  f"金额不匹配：期望={actual_amount}, 实际={auto1.get('amount')}", auto1)
                    return False
                self.log_step(3, "AUTO_TRANSFER_INNER (Receive->Redeem)", "PASS",
                              f"状态为 OUT_DONE_SUCCESS，金额匹配", auto1)
                break
            time.sleep(interval)
        if not auto1:
            self.log_step(3, "AUTO_TRANSFER_INNER (Receive->Redeem)", "FAIL", "未找到或未到达终态")
            return False

        # Step 4: REDEEM_TRANS_OUT（Redeem -> Burn）
        burn_order = f"{order_prefix}_BURN"
        print(f"[Step 4] 检查 REDEEM_TRANS_OUT (Redeem->Burn), order_id={burn_order}...")
        redeem_out = None
        while time.time() < deadline:
            redeem_out = get_redeem_trans_out_by_order_prefix(self.db_key, order_prefix)
            if self.report:
                self.report.add_db_query(self.db_key, "get_redeem_trans_out_by_order_prefix",
                                         f"order_prefix={order_prefix}", result=redeem_out, hit=bool(redeem_out))
            if redeem_out and redeem_out.get("status_flow") == "OUT_DONE_SUCCESS":
                msg_orig = redeem_out.get("message_orig", "")
                try:
                    msg_data = json.loads(msg_orig) if isinstance(msg_orig, str) else msg_orig
                    redeem_order_id = msg_data.get("redeemTokenRequestDto", {}).get("orderId", "")
                    if redeem_order_id != order_prefix:
                        self.log_step(4, "REDEEM_TRANS_OUT 赎回订单核对", "FAIL",
                                      f"redeem orderId 不匹配：期望={order_prefix}, 实际={redeem_order_id}", redeem_out)
                        return False
                except Exception:
                    pass
                self.log_step(4, "REDEEM_TRANS_OUT (Redeem->Burn)", "PASS",
                              f"状态为 OUT_DONE_SUCCESS，orderId 核对正确", redeem_out)
                break
            time.sleep(interval)
        if not redeem_out:
            self.log_step(4, "REDEEM_TRANS_OUT (Redeem->Burn)", "FAIL", "未找到或未到达终态")
            return False

        # Step 5: INFO_TRANS_IN（REDEEM_RESP）
        print(f"[Step 5] 检查 INFO_TRANS_IN (REDEEM_RESP), order_id={order_prefix}...")
        redeem_out_ct = redeem_out.get("ct") if redeem_out else None
        info_in = None
        while time.time() < deadline:
            candidates = get_info_trans_in_candidates(self.db_key, min_ct=redeem_out_ct, limit=20)
            if self.report:
                self.report.add_db_query(self.db_key, "get_info_trans_in_candidates",
                                         f"order_prefix={order_prefix}", result=candidates, hit=bool(candidates))
            for row in candidates:
                if row.get("status_flow") != "INFO_IN":
                    continue
                memo = row.get("memo") or ""
                try:
                    memo_data = json.loads(memo) if isinstance(memo, str) else memo
                    content = json.loads(memo_data.get("content", "{}"))
                    inner_order_id = content.get("orderId", "")
                    if inner_order_id != order_prefix:
                        continue
                    status = content.get("status", "")
                    description = content.get("description", "")
                    token_amount = content.get("tokenAmount", "")
                    if status != "COMPLETED":
                        self.log_step(5, "INFO_TRANS_IN (REDEEM_RESP)", "FAIL",
                                      f"status 非 COMPLETED：{status} - {description}", row)
                        return False
                    self.log_step(5, "INFO_TRANS_IN (REDEEM_RESP)", "PASS",
                                  f"status=COMPLETED, tokenAmount={token_amount}", row)
                    info_in = row
                    break
                except Exception as e:
                    continue
            if info_in:
                break
            time.sleep(interval)
        if not info_in:
            self.log_step(5, "INFO_TRANS_IN (REDEEM_RESP)", "FAIL", "未找到或未到达终态")
            return False

        return True


def verify_agent_fxb_flow(db_key: str, bank_req_id: str, amount: float, coin_id: str):
    verifier = AgentFxbVerifier(db_key, bank_req_id, amount, coin_id)
    success = verifier.verify_all()
    return success, verifier.results
