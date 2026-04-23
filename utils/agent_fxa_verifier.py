import time
import json
import os
import pymysql
from typing import Dict, Any, Optional
from utils.es_log_checker import EsLogChecker
from utils.query_db_data import (
    get_read_record,
    get_common_trans_out,
    get_info_trans_by_memo,
    get_mint_transfer_in,
    get_auto_transfer_inner,
)

try:
    from utils.report_writer import UnifiedReportWriter
except ImportError:
    UnifiedReportWriter = None


_FXA_DB_CFG = {
    "host": "devandsitmysql.mysql.database.azure.com",
    "user": "dev",
    "password": "Bhy.980226275",
    "database": "remi-fxa",
    "port": 3306,
    "charset": "utf8mb4",
    "ssl": {"verify_cert": False, "check_hostname": False},
}


def _get_fxa_conn():
    return pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **_FXA_DB_CFG)


class AgentFxaVerifier:
    """
    Agent 不持币（FXA）自动化验证类。
    实现用户描述的 6 步数据库检查流程。
    """

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
        self.parallel_progress_state: Dict[str, str] = {}
        self.parallel_progress_history = []
        self.last_parallel_snapshot: Dict[str, Any] = {}
        self.read_started_at_epoch: Optional[float] = None
        self.read_started_at_str: Optional[str] = None
        self.read_deadline_at_str: Optional[str] = None
        self.base_timeout_s = int(os.getenv("VERIFY_TIMEOUT_SECONDS", "300"))
        self.timeout_multiplier = float(os.getenv("VERIFY_TIMEOUT_MULTIPLIER", "1"))
        if self.timeout_multiplier < 1:
            self.timeout_multiplier = 1
        self.poll_interval_s = int(os.getenv("VERIFY_POLL_INTERVAL_SECONDS", "10"))
        self.es_timeout_s = int(os.getenv("VERIFY_ES_TIMEOUT_SECONDS", "120"))
        self.es_interval_s = int(
            os.getenv("VERIFY_ES_POLL_INTERVAL_SECONDS", str(self.poll_interval_s))
        )
        self.terminal_status_max_wait_s = int(
            os.getenv("VERIFY_TERMINAL_STATUS_MAX_WAIT_SECONDS", "360")
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
            self._conn = _get_fxa_conn()
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
            "raw_data": raw_data
        }
        self.results.append(res)
        print(f"[Step {step_no}] {name}: {status} - {detail}")
        if self.report:
            self.report.add_step(step_no, name, status, detail, raw_data)

    def _add_poll_attempt(self, step_name: str, attempt: int, found: bool, record: Any = None):
        if self.report:
            self.report.add_poll_attempt(step_name, attempt, found, record)

    def _mark_parallel_progress(self, key: str, status: str) -> None:
        old = self.parallel_progress_state.get(key)
        if old == status:
            return
        self.parallel_progress_state[key] = status
        point = {
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "key": key,
            "status": status,
        }
        self.parallel_progress_history.append(point)
        print(f"[ACK等待并行追踪] {key} -> {status}")

    def _poll_related_transactions_during_ack(self) -> Dict[str, Any]:
        """在等待 RETURNED_ACK 期间，并行追踪 message_transaction_record 相关交易进展。"""
        snapshot: Dict[str, Any] = {}
        try:
            common_tx = get_common_trans_out(self.db_key, self.bank_req_id, conn=self.get_conn())
            if self.report:
                self.report.add_db_query(self.db_key, "get_common_trans_out",
                                         f"bank_req_id={self.bank_req_id}",
                                         result=common_tx, hit=bool(common_tx))
            if common_tx:
                common_status = common_tx.get("status_flow", "UNKNOWN")
                snapshot["COMMON_TRANS_OUT"] = common_status
                self._mark_parallel_progress("COMMON_TRANS_OUT", common_status)

            mint_req = get_info_trans_by_memo(self.db_key, self.order_id, "MINT_REQ", "OUT", conn=self.get_conn())
            if self.report:
                self.report.add_db_query(self.db_key, "get_info_trans_by_memo(MINT_REQ)",
                                         f"order_id={self.order_id}", result=mint_req, hit=bool(mint_req))
            if mint_req:
                mint_req_status = mint_req.get("status_flow", "UNKNOWN")
                snapshot["INFO_TRANS_OUT_MINT_REQ"] = mint_req_status
                self._mark_parallel_progress("INFO_TRANS_OUT_MINT_REQ", mint_req_status)

                memo = mint_req.get("memo") or ""
                wallet_address = None
                if memo:
                    memo_data = json.loads(memo)
                    content = json.loads(memo_data.get("content", "{}"))
                    wallet_address = content.get("walletAddress")

                if wallet_address:
                    mint_in = get_mint_transfer_in(self.db_key, wallet_address, self.coin_id, self.start_time, conn=self.get_conn())
                    if self.report:
                        self.report.add_db_query(self.db_key, "get_mint_transfer_in",
                                                 f"wallet={wallet_address}", result=mint_in, hit=bool(mint_in))
                    if mint_in:
                        mint_in_status = mint_in.get("status_flow", "UNKNOWN")
                        snapshot["MINT_TRANSFER_IN"] = mint_in_status
                        self._mark_parallel_progress("MINT_TRANSFER_IN", mint_in_status)

                        mint_addr = mint_in.get("address")
                        if mint_addr:
                            inner_1 = get_auto_transfer_inner(
                                    self.db_key, mint_addr, None, self.coin_id, self.start_time, conn=self.get_conn()
                                )
                            if self.report:
                                self.report.add_db_query(self.db_key, "get_auto_transfer_inner(MINT->LIQ)",
                                                         f"from={mint_addr}", result=inner_1, hit=bool(inner_1))
                            if inner_1:
                                inner_1_status = inner_1.get("status_flow", "UNKNOWN")
                                snapshot["AUTO_TRANSFER_INNER_MINT_TO_LIQ"] = inner_1_status
                                self._mark_parallel_progress(
                                    "AUTO_TRANSFER_INNER_MINT_TO_LIQ", inner_1_status
                                )

                                liq_addr = inner_1.get("cp_address")
                                if liq_addr:
                                    inner_2 = get_auto_transfer_inner(
                                        self.db_key, liq_addr, None, self.coin_id, self.start_time, conn=self.get_conn()
                                    )
                                    if self.report:
                                        self.report.add_db_query(self.db_key, "get_auto_transfer_inner(LIQ->PAY)",
                                                                 f"from={liq_addr}", result=inner_2, hit=bool(inner_2))
                                    if inner_2:
                                        inner_2_status = inner_2.get("status_flow", "UNKNOWN")
                                        snapshot["AUTO_TRANSFER_INNER_LIQ_TO_PAY"] = inner_2_status
                                        self._mark_parallel_progress(
                                            "AUTO_TRANSFER_INNER_LIQ_TO_PAY", inner_2_status
                                        )

            mint_resp = get_info_trans_by_memo(self.db_key, self.order_id, "MINT_RESP", "IN", conn=self.get_conn())
            if self.report:
                self.report.add_db_query(self.db_key, "get_info_trans_by_memo(MINT_RESP)",
                                         f"order_id={self.order_id}", result=mint_resp, hit=bool(mint_resp))
            if mint_resp:
                mint_resp_status = mint_resp.get("status_flow", "UNKNOWN")
                snapshot["INFO_TRANS_IN_MINT_RESP"] = mint_resp_status
                self._mark_parallel_progress("INFO_TRANS_IN_MINT_RESP", mint_resp_status)
        except Exception as e:
            snapshot["parallel_poll_error"] = str(e)

        return snapshot

    @staticmethod
    def _has_processing_transaction(snapshot: Dict[str, Any]) -> bool:
        processing_keys = (
            "PROCESSING",
            "PENDING",
            "READ",
            "INIT",
            "OUT_SEND",
            "IN_RESULT",
        )
        for value in snapshot.values():
            if not isinstance(value, str):
                continue
            value_upper = value.upper()
            if any(k.upper() in value_upper for k in processing_keys):
                return True
        return False

    def _check_es_callback(self, expect_onchain_status: str, require_fail_reason: bool) -> Dict[str, Any]:
        return self.es_checker.poll_callback_log(
            order_id=self.order_id,
            api_keyword="updateOnchainStatus",
            expect_onchain_status=expect_onchain_status,
            require_fail_reason=require_fail_reason,
            time_from="now-30m",
            timeout_s=self.es_timeout_s,
            interval_s=self.es_interval_s,
            size=200,
        )

    def _handle_terminal_status_timeout(self, record: Dict[str, Any]) -> bool:
        """
        6 分钟超时后的处理逻辑：
        1. 检查是否存在 processing 状态的关联交易
        2. 若有 → 第一次延长 5 分钟，第二次再延长 3 分钟
        3. 若无 → 立即终止轮询并返回 False
        """
        EXTENSION_ROUNDS = [
            ("1st_extension", 5 * 60, "有 processing 交易记录，延长 5 分钟..."),
            ("2nd_extension", 3 * 60, "仍有 processing 交易记录，继续延长 3 分钟..."),
        ]
        for ext_name, ext_seconds, ext_log_msg in EXTENSION_ROUNDS:
            self.last_parallel_snapshot = self._poll_related_transactions_during_ack()
            snapshot_keys = list(self.last_parallel_snapshot.keys())
            has_proc = self._has_processing_transaction(self.last_parallel_snapshot)
            print(f"[{ext_name}] snapshot_keys={snapshot_keys}, snapshot内容: {self.last_parallel_snapshot}, has_processing={has_proc}")
            if has_proc:
                new_deadline = time.time() + ext_seconds
                self.read_deadline_at_str = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(new_deadline)
                )
                print(f"[{ext_name}] {ext_log_msg} 新截止时间: {self.read_deadline_at_str}")
                while time.time() < new_deadline:
                    record = get_read_record(self.db_key, self.order_id, conn=self.get_conn())
                    if record:
                        status = record.get("status")
                        if status in ("RETURNED_ACK", "RETURNED_NAK"):
                            return True
                    self.last_parallel_snapshot = self._poll_related_transactions_during_ack()
                    if not self._has_processing_transaction(self.last_parallel_snapshot):
                        print("检测到所有交易已处理完毕，终止轮询。")
                        self._finalize_step1_timeout(record, "终止原因：无 processing 交易记录")
                        return False
                    time.sleep(self.poll_interval_s)
            else:
                print("无 processing 交易记录，终止轮询。")
                self._finalize_step1_timeout(record, "终止原因：无 processing 交易记录")
                return False

        self._finalize_step1_timeout(
            record,
            f"已延长 2 次（共 8 分钟）仍未到达终态，终止轮询",
        )
        return False

    def _finalize_step1_timeout(self, record: Dict[str, Any], termination_reason: str):
        self.log_step(
            1,
            "read_record 状态检查",
            "TIMEOUT",
            termination_reason,
            {
                "read_record": record,
                "read_started_at": self.read_started_at_str,
                "read_deadline_at": self.read_deadline_at_str,
                "parallel_progress_last_snapshot": self.last_parallel_snapshot,
                "parallel_progress_history": self.parallel_progress_history,
            },
        )

    def verify_all(self, timeout: Optional[int] = None, interval: Optional[int] = None):
        timeout_s = timeout if timeout is not None else self.base_timeout_s
        interval_s = interval if interval is not None else self.poll_interval_s
        effective_timeout = int(timeout_s * self.timeout_multiplier)
        print(
            f"验证参数: timeout={timeout_s}s, multiplier={self.timeout_multiplier}, "
            f"effective_timeout={effective_timeout}s, poll_interval={interval_s}s, "
            f"es_timeout={self.es_timeout_s}s, es_interval={self.es_interval_s}s, "
            f"terminal_status_max_wait={self.terminal_status_max_wait_s}s"
        )
        deadline = time.time() + effective_timeout
        read_started_at = None
        step1_passed = False
        while time.time() < deadline:
            # 在等待 ACK 的同时并行轮询相关交易进展
            self.last_parallel_snapshot = self._poll_related_transactions_during_ack()

            record = get_read_record(self.db_key, self.order_id, conn=self.get_conn())
            if self.report:
                self.report.add_db_query(self.db_key, "get_read_record",
                                         f"order_id={self.order_id}", result=record, hit=bool(record))
            if record:
                status = record.get("status")
                if status == "READ" and read_started_at is None:
                    read_started_at = time.time()
                    self.read_started_at_epoch = read_started_at
                    self.read_started_at_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(read_started_at))
                    self.read_deadline_at_str = time.strftime(
                        "%Y-%m-%d %H:%M:%S",
                        time.localtime(read_started_at + self.terminal_status_max_wait_s),
                    )
                    print(
                        "检测到 read_record 进入 READ，"
                        f"开始 {self.terminal_status_max_wait_s}s 终态倒计时..."
                    )
                    print(
                        f"READ started_at={self.read_started_at_str}, "
                        f"deadline_at={self.read_deadline_at_str}"
                    )

                if read_started_at is not None and time.time() - read_started_at >= self.terminal_status_max_wait_s:
                    reached = self._handle_terminal_status_timeout(
                        record,
                    )
                    if reached:
                        step1_passed = True
                        break
                    return False

                if status == "RETURNED_ACK":
                    es_ack = self._check_es_callback("ACK", require_fail_reason=False)
                    if self.report:
                        self.report.add_es_query("kibana_proxy", self.order_id, "updateOnchainStatus ACK",
                                                 "now-30m", es_ack.get("hit_count", 0),
                                                 f"expect_onchain_status=ACK, result={es_ack.get('status')}")
                    if es_ack.get("status") != "PASS":
                        self.log_step(
                            9,
                            "ES 回调校验(ACK)",
                            "FAIL",
                            "RETURNED_ACK 后未查询到下游 ACK 回调日志",
                            es_ack,
                        )
                        return False
                    self.log_step(9, "ES 回调校验(ACK)", "PASS", "已查询到下游 ACK 回调日志", es_ack)
                    self.log_step(
                        1,
                        "read_record 状态检查",
                        "PASS",
                        f"状态已到达 {status}",
                        {
                            "read_record": record,
                            "read_started_at": self.read_started_at_str,
                            "read_deadline_at": self.read_deadline_at_str,
                            "parallel_progress_last_snapshot": self.last_parallel_snapshot,
                            "parallel_progress_history": self.parallel_progress_history,
                        },
                    )
                    step1_passed = True
                    break
                elif status == "RETURNED_NAK":
                    es_nak = self._check_es_callback("NAK", require_fail_reason=True)
                    if self.report:
                        self.report.add_es_query("kibana_proxy", self.order_id, "updateOnchainStatus NAK",
                                                 "now-30m", es_nak.get("hit_count", 0),
                                                 f"expect_onchain_status=NAK, result={es_nak.get('status')}")
                    if es_nak.get("status") != "PASS":
                        self.log_step(
                            8,
                            "ES 回调校验(NAK+失败原因)",
                            "FAIL",
                            "检测到 RETURNED_NAK，但未查询到下游 NAK 回调或缺失失败原因",
                            es_nak,
                        )
                        return False
                    self.log_step(
                        8,
                        "ES 回调校验(NAK+失败原因)",
                        "PASS",
                        "检测到 RETURNED_NAK，且下游已收到 NAK 回调与失败原因",
                        es_nak,
                    )
                    self.log_step(
                        1,
                        "read_record 状态检查",
                        "FAIL",
                        f"状态为 {status}（NAK=交易失败，不代表全链路成功）",
                        {
                            "read_record": record,
                            "read_started_at": self.read_started_at_str,
                            "read_deadline_at": self.read_deadline_at_str,
                            "parallel_progress_last_snapshot": self.last_parallel_snapshot,
                            "parallel_progress_history": self.parallel_progress_history,
                        },
                    )
                    # NAK = 交易本身失败，测试结果应为 FAIL
                    return False
            else:
                if self._has_processing_transaction(self.last_parallel_snapshot):
                    print("read_record 未到终态，关联交易处理中，继续轮询...")
                else:
                    print("read_record 未到终态，且暂未观察到处理中交易，继续轮询...")
            time.sleep(interval_s)
        if not step1_passed:
            self.log_step(
                1,
                "read_record 状态检查",
                "TIMEOUT",
                f"超时未到达 RETURNED_ACK（全局超时已放大至 {effective_timeout}s）",
                {
                    "read_started_at": self.read_started_at_str,
                    "read_deadline_at": self.read_deadline_at_str,
                    "parallel_progress_last_snapshot": self.last_parallel_snapshot,
                    "parallel_progress_history": self.parallel_progress_history,
                },
            )
            return False

        # Step 2: COMMON_TRANS_OUT 检查
        print(f"开始检查 COMMON_TRANS_OUT, bank_req_id={self.bank_req_id}...")
        common_tx = None
        while time.time() < deadline:
            common_tx = get_common_trans_out(self.db_key, self.bank_req_id, conn=self.get_conn())
            if common_tx and common_tx.get("status_flow") == "OUT_DONE_BACK_ACK":
                self.log_step(2, "COMMON_TRANS_OUT 终态检查", "PASS", "状态为 OUT_DONE_BACK_ACK", common_tx)
                break
            time.sleep(interval_s)
        if not common_tx or common_tx.get("status_flow") != "OUT_DONE_BACK_ACK":
            # 这里先不 return False，因为主报文状态可能会在后面才变成 ACK，
            # 但为了严格匹配用户步骤，我们继续检查其他 5 笔，最后再看这笔
            self.log_step(2, "COMMON_TRANS_OUT 终态检查", "PENDING", "主报文尚未到达终态，继续检查后续步骤", common_tx)

        # Step 3: INFO_TRANS_OUT (MINT_REQ) 检查
        print(f"开始检查 INFO_TRANS_OUT (MINT_REQ), order_id={self.order_id}...")
        mint_req = None
        while time.time() < deadline:
            mint_req = get_info_trans_by_memo(self.db_key, self.order_id, "MINT_REQ", "OUT", conn=self.get_conn())
            if mint_req and mint_req.get("status_flow") == "OUT_DONE_SUCCESS":
                self.log_step(3, "INFO_TRANS_OUT (铸币请求) 检查", "PASS", "状态为 OUT_DONE_SUCCESS", mint_req)
                break
            time.sleep(interval_s)
        if not mint_req:
            self.log_step(3, "INFO_TRANS_OUT (铸币请求) 检查", "FAIL", "未找到 MINT_REQ 记录")
            return False

        # Step 4: MINT_TRANSFER_IN 检查
        # 需要从 MINT_REQ 中获取钱包地址
        memo_data = json.loads(mint_req["memo"])
        content = json.loads(memo_data["content"])
        wallet_address = content["walletAddress"]
        
        print(f"开始检查 MINT_TRANSFER_IN, address={wallet_address}...")
        mint_in = None
        while time.time() < deadline:
            mint_in = get_mint_transfer_in(self.db_key, wallet_address, self.coin_id, self.start_time, conn=self.get_conn())
            if mint_in and mint_in.get("status_flow") == "IN_MINT_DONE":
                # 校验金额
                if float(mint_in["amount"]) == float(self.amount):
                    self.log_step(4, "MINT_TRANSFER_IN (铸币汇入) 检查", "PASS", f"状态为 IN_MINT_DONE, 金额 {self.amount} 匹配", mint_in)
                    break
                else:
                    self.log_step(4, "MINT_TRANSFER_IN (铸币汇入) 检查", "FAIL", f"金额不匹配: 预期 {self.amount}, 实际 {mint_in['amount']}", mint_in)
                    return False
            time.sleep(interval_s)
        if not mint_in:
            self.log_step(4, "MINT_TRANSFER_IN (铸币汇入) 检查", "TIMEOUT", "超时未找到 MINT_TRANSFER_IN 记录")
            return False

        # Step 5: INFO_TRANS_IN (MINT_RESP) 检查
        print(f"开始检查 INFO_TRANS_IN (MINT_RESP), order_id={self.order_id}...")
        mint_resp = None
        while time.time() < deadline:
            mint_resp = get_info_trans_by_memo(self.db_key, self.order_id, "MINT_RESP", "IN", conn=self.get_conn())
            if mint_resp and mint_resp.get("status_flow") == "INFO_IN":
                # 关键关联校验：MINT_TRANSFER_IN.tx_id 必须与 INFO_TRANS_IN.memo.content.txHash 对应
                try:
                    memo_data = json.loads(mint_resp.get("memo") or "{}")
                    content_raw = memo_data.get("content", "{}")
                    if isinstance(content_raw, str):
                        content = json.loads(content_raw or "{}")
                    elif isinstance(content_raw, dict):
                        content = content_raw
                    else:
                        content = {}
                    tx_hash = content.get("txHash")
                except Exception as e:
                    self.log_step(
                        5,
                        "INFO_TRANS_IN (铸币响应) 检查",
                        "FAIL",
                        f"解析 memo.txHash 失败: {e}",
                        mint_resp,
                    )
                    return False

                mint_tx_id = mint_in.get("tx_id")
                if not tx_hash:
                    self.log_step(
                        5,
                        "INFO_TRANS_IN (铸币响应) 检查",
                        "FAIL",
                        "未找到 INFO_TRANS_IN.memo.content.txHash",
                        mint_resp,
                    )
                    return False

                if mint_tx_id != tx_hash:
                    self.log_step(
                        5,
                        "INFO_TRANS_IN (铸币响应) 检查",
                        "FAIL",
                        f"tx_id 不匹配: MINT_TRANSFER_IN.tx_id={mint_tx_id}, INFO_TRANS_IN.txHash={tx_hash}",
                        {
                            "mint_transfer_in": mint_in,
                            "info_trans_in": mint_resp,
                        },
                    )
                    return False

                self.log_step(
                    5,
                    "INFO_TRANS_IN (铸币响应) 检查",
                    "PASS",
                    "状态为 INFO_IN，且 MINT_TRANSFER_IN.tx_id 与 INFO_TRANS_IN.txHash 一致",
                    {
                        "mint_transfer_in_tx_id": mint_tx_id,
                        "info_trans_in_tx_hash": tx_hash,
                        "info_trans_in": mint_resp,
                    },
                )
                break
            time.sleep(interval_s)
        if not mint_resp:
            self.log_step(5, "INFO_TRANS_IN (铸币响应) 检查", "TIMEOUT", "超时未找到 MINT_RESP 记录")
            return False

        # Step 6: 内部划拨 1 (Mint -> Liquidity)
        # 获取 Mint 地址和 Liquidity 地址
        mint_addr = mint_in["address"]
        print(f"开始检查 AUTO_TRANSFER_INNER (Mint -> Liquidity), from={mint_addr}...")
        inner1 = None
        while time.time() < deadline:
            inner1 = get_auto_transfer_inner(self.db_key, mint_addr, None, self.coin_id, self.start_time, conn=self.get_conn())
            if inner1 and inner1.get("status_flow") == "OUT_DONE_SUCCESS":
                if float(inner1["amount"]) == float(self.amount):
                    self.log_step(6, "内部划拨 (Mint -> Liquidity) 检查", "PASS", "状态为 OUT_DONE_SUCCESS, 金额匹配", inner1)
                    break
            time.sleep(interval_s)
        if not inner1:
            self.log_step(6, "内部划拨 (Mint -> Liquidity) 检查", "TIMEOUT", "超时未找到划拨记录")
            return False

        # Step 7: 内部划拨 2 (Liquidity -> Pay)
        liq_addr = inner1["cp_address"]
        print(f"开始检查 AUTO_TRANSFER_INNER (Liquidity -> Pay), from={liq_addr}...")
        inner2 = None
        while time.time() < deadline:
            inner2 = get_auto_transfer_inner(self.db_key, liq_addr, None, self.coin_id, self.start_time, conn=self.get_conn())
            if inner2 and inner2.get("status_flow") == "OUT_DONE_SUCCESS":
                if float(inner2["amount"]) == float(self.amount):
                    self.log_step(7, "内部划拨 (Liquidity -> Pay) 检查", "PASS", "状态为 OUT_DONE_SUCCESS, 金额匹配", inner2)
                    break
            time.sleep(interval_s)
        if not inner2:
            self.log_step(7, "内部划拨 (Liquidity -> Pay) 检查", "TIMEOUT", "超时未找到划拨记录")
            return False

        # 最后再次检查 Step 2 (主报文终态)
        print("最后确认主报文终态...")
        while time.time() < deadline:
            common_tx = get_common_trans_out(self.db_key, self.bank_req_id)
            if common_tx and common_tx.get("status_flow") == "OUT_DONE_BACK_ACK":
                # 更新之前的步骤 2 结果
                for r in self.results:
                    if r["step"] == 2:
                        r["status"] = "PASS"
                        r["detail"] = "状态已到达 OUT_DONE_BACK_ACK"
                        r["raw_data"] = common_tx
                print("[Step 2] COMMON_TRANS_OUT 终态检查: PASS - 状态已到达 OUT_DONE_BACK_ACK")
                return True
            time.sleep(interval_s)
        
        self.log_step(2, "COMMON_TRANS_OUT 终态确认", "FAIL", "主报文最终未到达 OUT_DONE_BACK_ACK", common_tx)
        return False

def verify_agent_fxa_flow(db_key: str, bank_req_id: str, order_id: str, amount: float, coin_id: str):
    verifier = AgentFxaVerifier(db_key, bank_req_id, order_id, amount, coin_id)
    success = verifier.verify_all()
    return success, verifier.results
