import json
import time
import html
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

class UnifiedReportWriter:
    def __init__(self, case_name: str, debug_level: str = "summary"):
        self.case_name = case_name
        self.debug_level = debug_level
        self.started_at = time.strftime("%Y-%m-%d %H:%M:%S")
        self.finished_at: str = ""
        self.duration_seconds: float = 0.0
        self.final_status: str = "FAIL"
        self.failure_reason: str = ""
        self.http_calls: List[Dict[str, Any]] = []
        self.db_queries: List[Dict[str, Any]] = []
        self.es_queries: List[Dict[str, Any]] = []
        self.step_results: List[Dict[str, Any]] = []
        self.poll_attempts: List[Dict[str, Any]] = []
        self.console_logs: str = ""
        self.request_context: Dict[str, Any] = {}
        self._db_throttle: Dict[str, float] = {}
        self._db_interval = 30.0
        self._es_throttle: Dict[str, float] = {}

    def set_request_context(self, ctx: Dict[str, Any]):
        self.request_context = ctx

    def add_http_call(self, step: str, method: str, url: str,
                      request_headers: Dict, request_body: Any,
                      response_status: int, response_body: str):
        self.http_calls.append({
            "step": step,
            "method": method,
            "url": url,
            "request_headers": request_headers,
            "request_body": request_body,
            "response_status": response_status,
            "response_body": response_body,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        })

    def add_db_query(self, db_type: str, action: str, sql: str,
                     params: Any = None, result: Any = None,
                     hit: bool = True, detail: str = ""):
        now = time.time()
        key = (db_type, action, sql, str(params))
        last = self._db_throttle.get(key, 0)
        if now - last < self._db_interval:
            return
        self._db_throttle[key] = now
        record = {
            "db_type": db_type,
            "action": action,
            "sql": sql,
            "params": params,
            "hit": hit,
            "detail": detail,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        if isinstance(result, dict):
            record["row_count"] = 1
            record["status_flow"] = result.get("status_flow") or result.get("status") or ""
        elif isinstance(result, list):
            record["row_count"] = len(result)
            record["status_flow"] = ""
        self.db_queries.append(record)

    def add_es_query(self, source: str, order_id: str, keyword: str,
                     time_from: str, hit_count: int, detail: str = ""):
        now = time.time()
        key = (source, order_id, keyword, time_from)
        last = self._es_throttle.get(key, 0)
        if now - last < self._db_interval:
            return
        self._es_throttle[key] = now
        self.es_queries.append({
            "source": source,
            "order_id": order_id,
            "keyword": keyword,
            "time_from": time_from,
            "hit_count": hit_count,
            "detail": detail,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        })

    def add_step(self, step_no: int, name: str, status: str,
                 detail: str, raw_data: Any = None):
        self.step_results.append({
            "step": step_no,
            "name": name,
            "status": status,
            "detail": detail,
            "raw_data": raw_data,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        })

    def add_poll_attempt(self, step_name: str, attempt: int,
                         found: bool, record: Any = None):
        self.poll_attempts.append({
            "step_name": step_name,
            "attempt": attempt,
            "found": found,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "summary": str(record.get("status_flow", "") if isinstance(record, dict) else ""),
        })

    def finalize(self, final_status: str, failure_reason: str = "",
                 console_logs: str = ""):
        self.finished_at = time.strftime("%Y-%m-%d %H:%M:%S")
        self.final_status = final_status
        self.failure_reason = failure_reason
        self.console_logs = console_logs

    def to_dict(self) -> Dict[str, Any]:
        return {
            "meta": {
                "case_name": self.case_name,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "duration_seconds": self.duration_seconds,
                "debug_level": self.debug_level,
            },
            "request_context": self.request_context,
            "http_calls": self.http_calls,
            "db_queries": self.db_queries,
            "es_queries": self.es_queries,
            "step_results": self.step_results,
            "poll_attempts": self.poll_attempts,
            "final_status": self.final_status,
            "failure_reason": self.failure_reason,
            "console_logs": self.console_logs,
        }

    def write(self, suffix: str = "") -> tuple:
        report_dir = Path(__file__).resolve().parent.parent / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        name_base = f"{self.case_name}_{suffix}_{ts}" if suffix else f"{self.case_name}_{ts}"

        json_path = report_dir / f"{name_base}.json"
        html_path = report_dir / f"{name_base}.html"

        json_path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8"
        )
        html_path.write_text(self.build_multiflow_html(), encoding="utf-8")
        return str(json_path), str(html_path)

    def build_unified_html(self) -> str:
        fc = html.escape

        status_cfg = {
            "PASS":    {"label": "PASS",    "color": "#22c55e", "bg": "rgba(34,197,94,.12)",  "border": "rgba(34,197,94,.3)"},
            "FAIL":    {"label": "FAIL",    "color": "#ef4444", "bg": "rgba(239,68,68,.12)",   "border": "rgba(239,68,68,.3)"},
            "ERROR":   {"label": "ERROR",   "color": "#f97316", "bg": "rgba(249,115,22,.12)",  "border": "rgba(249,115,22,.3)"},
            "TIMEOUT": {"label": "TIMEOUT","color": "#eab308", "bg": "rgba(234,179,8,.12)",    "border": "rgba(234,179,8,.3)"},
            "BLOCKED": {"label": "BLOCKED","color": "#6b7280", "bg": "rgba(107,114,128,.12)",  "border": "rgba(107,114,128,.3)"},
            "":        {"label": "—",       "color": "#6b7280", "bg": "rgba(107,114,128,.12)",  "border": "rgba(107,114,128,.3)"},
        }
        cfg = status_cfg.get(self.final_status, status_cfg[""])
        st_color = cfg["color"]
        st_bg = cfg["bg"]
        st_border = cfg["border"]

        if self.final_status in ("FAIL", "ERROR", "TIMEOUT"):
            failed = [s for s in self.step_results if s["status"] in ("FAIL", "ERROR")]
            fail_items = ""
            for f in failed:
                fail_items += (
                    "<li>"
                    "<span class='fail-step'>Step " + str(f.get("step","")) + "</span>"
                    "<span class='fail-name'>" + fc(str(f.get("name",""))) + "</span>"
                    "<span class='fail-detail'>" + fc(str(f.get("detail",""))[:120]) + "</span>"
                    "</li>"
                )
            alert_banner = (
                "<div class='alert-banner' style='background:" + st_bg + ";border-color:" + st_border + "'>"
                  "<div class='alert-title'>"
                    "<span class='alert-dot' style='background:" + st_color + "'></span>"
                    + cfg["label"] + " — " + str(len(failed)) + " 个核查步骤失败"
                  "</div>"
                  "<ul class='fail-quick-list'>" + fail_items + "</ul>"
                "</div>"
            )
        else:
            alert_banner = ""

        steps_pass = sum(1 for s in self.step_results if s["status"] == "PASS")
        steps_total = len(self.step_results)
        stat_bar = (
            "<div class='stat-bar'>"
              "<div class='sitem'><span class='slabel'>HTTP</span><span class='sval'>" + str(len(self.http_calls)) + "</span></div>"
              "<div class='sitem'><span class='slabel'>DB查询</span><span class='sval'>" + str(len(self.db_queries)) + "</span></div>"
              "<div class='sitem'><span class='slabel'>ES查询</span><span class='sval'>" + str(len(self.es_queries)) + "</span></div>"
              "<div class='sitem'><span class='slabel'>步骤</span><span class='sval'>" + str(steps_pass) + "/" + str(steps_total) + "</span></div>"
              "<div class='sitem'><span class='slabel'>耗时</span><span class='sval'>" + ("%.1f" % self.duration_seconds) + "s</span></div>"
              "<div class='sitem sitem-status' style='background:" + st_bg + ";border-color:" + st_border + "'>"
                "<span class='slabel'>状态</span>"
                "<span class='sval' style='color:" + st_color + "'>" + cfg["label"] + "</span>"
              "</div>"
            "</div>"
        )

        ctx_rows = ""
        for k, v in self.request_context.items():
            v_str = json.dumps(v, ensure_ascii=False, default=str)
            ctx_rows += "<tr><td class='ck'>" + fc(str(k)) + "</td><td class='cv'><details><summary>查看</summary><pre>" + fc(v_str) + "</pre></details></td></tr>"
        ctx_sec = (
            "<div class='section'>"
              "<div class='sec-title' onclick='toggleSec(this)'>请求上下文 <span class='sec-toggle'>▸</span></div>"
              "<div class='sec-body'>"
                "<table class='dtable'><thead><tr><th>字段</th><th>值</th></tr></thead>"
                "<tbody>" + (ctx_rows or "<tr><td colspan='2' class='empty'>无</td></tr>") + "</tbody></table>"
              "</div>"
            "</div>"
        )

        http_rows = ""
        for c in self.http_calls:
            req = fc(json.dumps(c.get("request_body",""), ensure_ascii=False, default=str))
            resp = fc(str(c.get("response_body","")))
            http_rows += (
                "<tr>"
                  "<td class='dt'>" + fc(str(c.get("timestamp",""))) + "</td>"
                  "<td class='dm'>" + fc(str(c.get("method",""))) + "</td>"
                  "<td class='du' title='" + fc(str(c.get("url",""))) + "'>" + fc(str(c.get("url",""))) + "</td>"
                  "<td class='ds'>" + fc(str(c.get("response_status",""))) + "</td>"
                  "<td class='dr'><details><summary>请求</summary><pre>" + req + "</pre></details></td>"
                  "<td class='dr'><details><summary>响应</summary><pre>" + resp + "</pre></details></td>"
                "</tr>"
            )
        http_sec = (
            "<div class='section'>"
              "<div class='sec-title' onclick='toggleSec(this)'>HTTP 请求 <span class='sec-badge'>" + str(len(self.http_calls)) + "</span> <span class='sec-toggle'>▸</span></div>"
              "<div class='sec-body'>"
                "<table class='dtable'><thead><tr><th>时间</th><th>Method</th><th>URL</th><th>Status</th><th>请求体</th><th>响应体</th></tr></thead>"
                "<tbody>" + (http_rows or "<tr><td colspan='6' class='empty'>无</td></tr>") + "</tbody></table>"
              "</div>"
            "</div>"
        )

        db_rows = ""
        for q in self.db_queries:
            sql = fc(str(q.get("sql","")))
            db_rows += (
                "<tr class='" + ("db-hit" if q.get("hit") else "db-miss") + "'>"
                  "<td class='dt'>" + fc(str(q.get("timestamp",""))) + "</td>"
                  "<td class='db-type'>" + fc(str(q.get("db_type",""))) + "</td>"
                  "<td class='db-act'>" + fc(str(q.get("action",""))) + "</td>"
                  "<td class='db-sql' title='" + sql + "'><details><summary>SQL</summary><pre>" + sql + "</pre></details></td>"
                  "<td class='db-hit-cell'>" + ("&#x2705;" if q.get("hit") else "&#x274C;") + "</td>"
                  "<td class='db-sf'>" + fc(str(q.get("status_flow",""))) + "</td>"
                "</tr>"
            )
        db_sec = (
            "<div class='section'>"
              "<div class='sec-title' onclick='toggleSec(this)'>DB 查询 <span class='sec-badge'>" + str(len(self.db_queries)) + "</span> <span class='sec-toggle'>▸</span></div>"
              "<div class='sec-body'>"
                "<table class='dtable'><thead><tr><th>时间</th><th>DB</th><th>Action</th><th>SQL</th><th>命中</th><th>Status</th></tr></thead>"
                "<tbody>" + (db_rows or "<tr><td colspan='6' class='empty'>无</td></tr>") + "</tbody></table>"
              "</div>"
            "</div>"
        )

        es_rows = ""
        for q in self.es_queries:
            es_rows += (
                "<tr class='" + ("es-hit" if q.get("hit_count") else "es-miss") + "'>"
                  "<td class='dt'>" + fc(str(q.get("timestamp",""))) + "</td>"
                  "<td class='db-type'>" + fc(str(q.get("source",""))) + "</td>"
                  "<td class='db-sql'><code>" + fc(str(q.get("order_id",""))) + "</code></td>"
                  "<td class='db-act'>" + fc(str(q.get("keyword",""))) + "</td>"
                  "<td class='db-hit-cell'>" + str(q.get("hit_count",0)) + "</td>"
                "</tr>"
            )
        es_sec = (
            "<div class='section'>"
              "<div class='sec-title' onclick='toggleSec(this)'>ES 日志查询 <span class='sec-badge'>" + str(len(self.es_queries)) + "</span> <span class='sec-toggle'>▸</span></div>"
              "<div class='sec-body'>"
                "<table class='dtable'><thead><tr><th>时间</th><th>Source</th><th>Order ID</th><th>Keyword</th><th>命中数</th></tr></thead>"
                "<tbody>" + (es_rows or "<tr><td colspan='5' class='empty'>无</td></tr>") + "</tbody></table>"
              "</div>"
            "</div>"
        )

        step_items = ""
        for s in self.step_results:
            sv = str(s.get("status","")).upper()
            dot_c = {"PASS":"#22c55e","FAIL":"#ef4444","ERROR":"#f97316","TIMEOUT":"#eab308"}.get(sv,"#6b7280")
            row_bg = {"PASS":"rgba(34,197,94,.06)","FAIL":"rgba(239,68,68,.08)","ERROR":"rgba(249,115,22,.08)","TIMEOUT":"rgba(234,179,8,.08)"}.get(sv,"transparent")
            raw = fc(json.dumps(s.get("raw_data",{}), ensure_ascii=False, default=str, indent=2))
            step_items += (
                "<tr class='step-row' style='background:" + row_bg + "'>"
                  "<td class='step-num'><span class='step-dot' style='background:" + dot_c + "'></span>" + str(s.get("step","")) + "</td>"
                  "<td class='step-name'>" + fc(str(s.get("name",""))) + "</td>"
                  "<td class='step-st' style='color:" + dot_c + "'>" + sv + "</td>"
                  "<td class='step-detail'>" + fc(str(s.get("detail",""))) + "</td>"
                  "<td class='step-raw'><details><summary>原始数据</summary><pre>" + raw + "</pre></details></td>"
                  "<td class='dt'>" + fc(str(s.get("timestamp",""))) + "</td>"
                "</tr>"
            )
        steps_sec = (
            "<div class='section'>"
              "<div class='sec-title'>核查步骤 <span class='sec-badge'>" + str(steps_pass) + "/" + str(steps_total) + " 通过</span></div>"
              "<div class='sec-body'>"
                "<table class='dtable step-table'><thead><tr><th>#</th><th>名称</th><th>状态</th><th>详情</th><th>原始数据</th><th>时间</th></tr></thead>"
                "<tbody>" + (step_items or "<tr><td colspan='6' class='empty'>无</td></tr>") + "</tbody></table>"
              "</div>"
            "</div>"
        )

        poll_rows = ""
        for p in self.poll_attempts:
            poll_rows += (
                "<tr class='" + ("poll-hit" if p.get("found") else "poll-miss") + "'>"
                  "<td class='dt'>" + fc(str(p.get("timestamp",""))) + "</td>"
                  "<td class='step-name'>" + fc(str(p.get("step_name",""))) + "</td>"
                  "<td class='step-num'>" + str(p.get("attempt","")) + "</td>"
                  "<td class='step-st'>" + ("HIT" if p.get("found") else "MISS") + "</td>"
                  "<td class='step-detail'>" + fc(str(p.get("summary",""))) + "</td>"
                "</tr>"
            )
        poll_sec = ""
        if poll_rows:
            poll_sec = (
                "<div class='section'>"
                  "<div class='sec-title' onclick='toggleSec(this)'>轮询记录 <span class='sec-badge'>" + str(len(self.poll_attempts)) + "</span> <span class='sec-toggle'>▸</span></div>"
                  "<div class='sec-body'>"
                    "<table class='dtable'><thead><tr><th>时间</th><th>检查项</th><th>轮次</th><th>命中</th><th>摘要</th></tr></thead>"
                    "<tbody>" + poll_rows + "</tbody></table>"
                  "</div>"
                "</div>"
            )

        log_txt = fc(self.console_logs[:8000])
        if len(self.console_logs) > 8000:
            log_txt += "\n... (truncated at 8000 chars)"
        log_sec = (
            "<div class='section'>"
              "<div class='sec-title' onclick='toggleSec(this)'>执行日志 <span class='sec-badge'>" + str(len(self.console_logs)) + " chars</span> <span class='sec-toggle'>▸</span></div>"
              "<div class='sec-body'><pre class='log-pre'>" + log_txt + "</pre></div>"
            "</div>"
        )

        return (
            "<!DOCTYPE html>"
            "<html lang='zh-CN'>"
            "<head>"
              "<meta charset='UTF-8'>"
              "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
              "<title>" + fc(self.case_name) + " — " + cfg["label"] + "</title>"
              "<style>"
                "*{box-sizing:border-box;margin:0;padding:0}"
                ":root{--bg:#0f1117;--s:#161a27;--s2:#1e2333;--s3:#252b3d;--bd:#2e3555;--tx:#d8dced;--tx2:#8890a8;--tx3:#4a4d6a;--pass:#22c55e;--fail:#ef4444;--warn:#eab308;--info:#6366f1;--pass-bg:rgba(34,197,94,.1);--fail-bg:rgba(239,68,68,.1);--warn-bg:rgba(234,179,8,.1);--font:ui-monospace,'Cascadia Code','Fira Code',Menlo,monospace}"
                "body{font-family:var(--font);background:var(--bg);color:var(--tx);font-size:13px;line-height:1.6}"
                "a{color:var(--info)}"
                ".case-hdr{background:linear-gradient(135deg,#0d1020,#141728 60%,#181b2e);border-bottom:1px solid var(--bd);padding:18px 24px}"
                ".case-hdr h1{font-size:15px;font-weight:700;color:#fff;margin-bottom:4px}"
                ".case-meta{font-size:11px;color:var(--tx2)}"
                ".case-meta span{margin-right:14px}"
                ".alert-banner{margin:16px 24px 0;border:1px solid var(--bd);border-radius:10px;padding:14px 18px;animation:fadeIn .3s ease}"
                "@keyframes fadeIn{from{opacity:0;transform:translateY(-4px)}}"
                ".alert-title{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:#fff;margin-bottom:10px}"
                ".alert-dot{width:8px;height:8px;border-radius:50%;display:inline-block}"
                ".fail-quick-list{list-style:none;display:flex;flex-direction:column;gap:5px}"
                ".fail-quick-list li{display:flex;align-items:baseline;gap:10px;background:rgba(0,0,0,.2);padding:6px 10px;border-radius:6px;font-size:12px}"
                ".fail-step{background:rgba(239,68,68,.2);color:#ef4444;padding:1px 7px;border-radius:6px;font-size:11px;flex-shrink:0;font-weight:700}"
                ".fail-name{color:var(--tx);font-weight:600;min-width:120px}"
                ".fail-detail{color:var(--tx2)}"
                ".stat-bar{display:flex;gap:8px;flex-wrap:wrap;margin:16px 24px 0;background:var(--s);border:1px solid var(--bd);border-radius:10px;padding:12px 16px}"
                ".sitem{background:var(--s2);border:1px solid var(--bd);border-radius:8px;padding:8px 14px;text-align:center;min-width:60px;flex:1}"
                ".sitem-status{border-color:" + st_border + "}"
                ".slabel{display:block;font-size:9px;color:var(--tx3);text-transform:uppercase;letter-spacing:.8px}"
                ".sval{display:block;font-size:18px;font-weight:800;color:#fff;margin-top:2px}"
                ".main{padding:16px 24px 32px}"
                ".section{background:var(--s);border:1px solid var(--bd);border-radius:10px;margin-bottom:12px;overflow:hidden}"
                ".sec-title{padding:11px 16px;font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.8px;cursor:pointer;user-select:none;display:flex;align-items:center;gap:8px;background:var(--s2);border-bottom:1px solid var(--bd);transition:background .15s}"
                ".sec-title:hover{background:var(--s3)}"
                ".sec-toggle{margin-left:auto;font-size:10px;transition:transform .2s}"
                ".sec-badge{background:var(--s3);color:var(--tx);padding:1px 8px;border-radius:8px;font-size:10px}"
                ".dtable{width:100%;border-collapse:collapse;font-size:12px;table-layout:auto}"
                ".dtable th{background:var(--s2);padding:8px 10px;text-align:left;font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.7px;border-bottom:1px solid var(--bd);white-space:nowrap}"
                ".dtable td{padding:7px 10px;border-bottom:1px solid rgba(46,53,85,.5);vertical-align:top;word-break:break-word}"
                ".dtable tr:last-child td{border-bottom:none}"
                ".dtable tr:hover{background:rgba(255,255,255,.02)}"
                ".empty{color:var(--tx3);font-style:italic}"
                ".ck{color:var(--tx3);font-size:11px;min-width:90px;white-space:nowrap}"
                "pre{white-space:pre-wrap;word-break:break-all;font-size:11px;background:var(--bg);padding:8px;border-radius:6px;margin:2px 0}"
                "details summary{cursor:pointer;color:var(--info);font-size:11px}"
                ".dt{color:var(--tx3);white-space:nowrap;font-size:11px}"
                ".dm{font-weight:700;color:var(--info);white-space:nowrap}"
                ".du{max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--tx)}"
                ".du:hover{white-space:normal;word-break:break-all}"
                ".ds{font-weight:700;white-space:nowrap}"
                ".db-type{color:var(--info);white-space:nowrap;font-size:11px}"
                ".db-act{color:var(--tx);white-space:nowrap}"
                ".db-sql{max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"
                ".db-sql:hover{white-space:normal;word-break:break-all}"
                ".db-hit-cell{font-size:16px;text-align:center}"
                ".db-sf{color:var(--tx2);white-space:nowrap;font-size:11px}"
                ".db-miss td{opacity:.6}"
                ".es-miss td{opacity:.5}"
                ".step-table .step-num{display:flex;align-items:center;gap:8px;white-space:nowrap;font-size:11px}"
                ".step-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;display:inline-block}"
                ".step-name{font-weight:600;white-space:nowrap}"
                ".step-st{font-weight:800;white-space:nowrap;font-size:11px}"
                ".step-detail{color:var(--tx2);max-width:300px}"
                ".step-raw{max-width:150px}"
                ".poll-miss td{opacity:.5}"
                ".log-pre{white-space:pre-wrap;word-break:break-all;font-size:11px;background:var(--bg);padding:12px;border-radius:6px;max-height:400px;overflow-y:auto}"
              "</style>"
            "</head>"
            "<body>"
              "<div class='case-hdr'>"
                "<h1>" + fc(self.case_name) + "</h1>"
                "<div class='case-meta'>"
                  "<span>⏱ " + fc(self.started_at) + " → " + fc(self.finished_at) + "</span>"
                  "<span>⏺ " + ("%.1f" % self.duration_seconds) + "s</span>"
                  "<span style='color:" + st_color + "'>● " + cfg["label"] + "</span>"
                  + (("<span style='color:var(--tx2)'>⚠ " + fc(self.failure_reason) + "</span>") if self.failure_reason else "")
                + "</div>"
              "</div>"
              + alert_banner
              + stat_bar
              + "<div class='main'>"
                + ctx_sec + http_sec + db_sec + es_sec + steps_sec + poll_sec + log_sec
              + "</div>"
              "<script>"
                "function toggleSec(el){"
                  "var body=el.nextElementSibling;"
                  "var tog=el.querySelector('.sec-toggle');"
                  "if(body.style.display==='none'){body.style.display='';tog.textContent='▾';}"
                  "else{body.style.display='none';tog.textContent='▸';}"
                "}"
                "document.querySelectorAll('.sec-body').forEach(function(b){b.style.display='';});"
                "document.querySelectorAll('.sec-toggle').forEach(function(t){t.textContent='▾';});"
              "</script>"
            "</body>"
            "</html>"
        )

class MultiFlowReportWriter:
    def __init__(self, run_id: str, run_name: str, flows: List[str],
                 msg_types: List[str], debug_level: str = "summary"):
        self.run_id = run_id
        self.run_name = run_name
        self.flows = flows
        self.msg_types = msg_types
        self.debug_level = debug_level
        self.started_at = time.strftime("%Y-%m-%d %H:%M:%S")
        self.finished_at = ""
        self.duration_seconds = 0.0
        self._reports: Dict[str, UnifiedReportWriter] = {}
        self.console_logs = ""

    def get_or_create_report(self, case_name: str) -> UnifiedReportWriter:
        if case_name not in self._reports:
            self._reports[case_name] = UnifiedReportWriter(case_name, self.debug_level)
        return self._reports[case_name]

    def finalize_all(self):
        self.finished_at = time.strftime("%Y-%m-%d %H:%M:%S")
        self.duration_seconds = max(0.1, time.time() - time.mktime(
            time.strptime(self.started_at, "%Y-%m-%d %H:%M:%S")))
        for r in self._reports.values():
            if not r.finished_at:
                r.finished_at = self.finished_at

    def write(self, suffix: str = "") -> tuple:
        report_dir = Path(__file__).resolve().parent.parent / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        name_base = f"regression_{self.run_name}_{self.run_id}_{suffix}"
        json_path = report_dir / f"{name_base}.json"
        html_path = report_dir / f"{name_base}.html"
        json_path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8"
        )
        html_path.write_text(self.build_multiflow_html(), encoding="utf-8")
        return str(json_path), str(html_path)

    def to_dict(self) -> Dict[str, Any]:
        passed = sum(1 for r in self._reports.values() if r.final_status == "PASS")
        failed = sum(1 for r in self._reports.values() if r.final_status == "FAIL")
        error = sum(1 for r in self._reports.values() if r.final_status == "ERROR")
        timeout = sum(1 for r in self._reports.values() if r.final_status == "TIMEOUT")
        return {
            "meta": {
                "run_id": self.run_id,
                "run_name": self.run_name,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "duration_seconds": round(self.duration_seconds, 1),
                "flows": self.flows,
                "msg_types": self.msg_types,
                "debug_level": self.debug_level,
            },
            "summary": {
                "total_flows": len(self._reports),
                "passed": passed,
                "failed": failed,
                "error": error,
                "timeout": timeout,
            },
            "flows": {k: r.to_dict() for k, r in self._reports.items()},
            "console_logs": self.console_logs,
        }

    def build_multiflow_html(self) -> str:
        fc = lambda s: html.escape(str(s)) if s else ""

        status_cfg = {
            "PASS":    {"label": "PASS",    "color": "#22c55e", "bg": "rgba(34,197,94,.12)",  "border": "rgba(34,197,94,.3)"},
            "FAIL":    {"label": "FAIL",    "color": "#ef4444", "bg": "rgba(239,68,68,.12)",   "border": "rgba(239,68,68,.3)"},
            "ERROR":   {"label": "ERROR",  "color": "#f97316", "bg": "rgba(249,115,22,.12)",  "border": "rgba(249,115,22,.3)"},
            "TIMEOUT": {"label": "TIMEOUT","color": "#eab308", "bg": "rgba(234,179,8,.12)",   "border": "rgba(234,179,8,.3)"},
            "":        {"label": "—",       "color": "#6b7280", "bg": "rgba(107,114,128,.12)", "border": "rgba(107,114,128,.3)"},
        }

        total = len(self._reports)
        passed = sum(1 for r in self._reports.values() if r.final_status == "PASS")
        failed = sum(1 for r in self._reports.values() if r.final_status in ("FAIL","ERROR"))
        timed_out = sum(1 for r in self._reports.values() if r.final_status == "TIMEOUT")
        pass_rate = (str(int(passed/total*100)) + "%") if total else "—"

        overall_cfg = status_cfg["PASS"] if failed == 0 and timed_out == 0 else (status_cfg["FAIL"] if failed > 0 else status_cfg["TIMEOUT"])

        stat_items = (
            "<div class='si' style='border-color:rgba(34,197,94,.3)'><div class='siv' style='color:#22c55e'>" + str(passed) + "</div><div class='sil'>通过</div></div>"
            "<div class='si' style='border-color:rgba(239,68,68,.3)'><div class='siv' style='color:#ef4444'>" + str(failed) + "</div><div class='sil'>失败</div></div>"
            "<div class='si' style='border-color:rgba(234,179,8,.3)'><div class='siv' style='color:#eab308'>" + str(timed_out) + "</div><div class='sil'>超时</div></div>"
            "<div class='si'><div class='siv'>" + str(total) + "</div><div class='sil'>总计</div></div>"
            "<div class='si' style='border-color:rgba(99,102,241,.35)'><div class='siv' style='color:#6366f1'>" + pass_rate + "</div><div class='sil'>通过率</div></div>"
            "<div class='si'><div class='siv'>" + str(int(round(self.duration_seconds,0))) + "s</div><div class='sil'>耗时</div></div>"
        )

        if failed > 0 or timed_out > 0:
            fail_flows = [(k,r) for k,r in self._reports.items() if r.final_status in ("FAIL","ERROR","TIMEOUT")]
            fail_items = ""
            for fname, r in fail_flows:
                cfg2 = status_cfg.get(r.final_status, status_cfg[""])
                fsteps = [s for s in r.step_results if s["status"] in ("FAIL","ERROR")]
                step_info = "; ".join(["Step " + str(s["step"]) + ":" + fc(str(s["name"])) for s in fsteps[:3]])
                fail_items += (
                    "<div class='mf-fail-item' style='background:" + cfg2["bg"] + ";border-color:" + cfg2["border"] + "'>"
                      "<span class='mf-fail-name' style='color:" + cfg2["color"] + "'>" + fc(fname) + "</span>"
                      "<span class='mf-fail-reason'>" + (step_info or fc(str(r.failure_reason or r.final_status))) + "</span>"
                    "</div>"
                )
            top_alert = (
                "<div class='mf-alert' style='background:" + overall_cfg["bg"] + ";border-color:" + overall_cfg["border"] + "'>"
                  "<div class='mf-alert-title'>"
                    "<span class='mf-alert-dot' style='background:" + overall_cfg["color"] + "'></span>"
                    + ("部分流程失败" if failed > 0 else "存在超时流程") + " — " + str(failed) + " 个失败, " + str(timed_out) + " 个超时"
                  "</div>"
                  "<div class='mf-fail-list'>" + fail_items + "</div>"
                "</div>"
            )
        else:
            top_alert = ""

        nav_pills = ""
        for name, r in self._reports.items():
            cfg2 = status_cfg.get(r.final_status, status_cfg[""])
            icon = {"PASS":"&#x2705;","FAIL":"&#x274C;","ERROR":"&#x26A0;","TIMEOUT":"&#x23F1;"}.get(r.final_status,"&#x25CF;")
            nav_pills += (
                "<button class='pill' style='border-color:" + cfg2["border"] + ";color:" + cfg2["color"] + ";background:" + cfg2["bg"] + "' "
                "onclick=\"document.getElementById('flow-" + fc(name) + "').scrollIntoView()\">"
                + icon + " " + fc(name) + "</button>"
            )

        flow_cards = ""
        for name, r in self._reports.items():
            cfg2 = status_cfg.get(r.final_status, status_cfg[""])
            sp = sum(1 for s in r.step_results if s["status"] == "PASS")
            st = len(r.step_results)
            icon = {"PASS":"&#x2705;","FAIL":"&#x274C;","ERROR":"&#x26A0;","TIMEOUT":"&#x23F1;"}.get(r.final_status,"&#x25CF;")

            step_rows = ""
            for s in r.step_results:
                sv = str(s.get("status","")).upper()
                dot_c = {"PASS":"#22c55e","FAIL":"#ef4444","ERROR":"#f97316","TIMEOUT":"#eab308"}.get(sv,"#6b7280")
                row_bg = {"PASS":"rgba(34,197,94,.06)","FAIL":"rgba(239,68,68,.07)","ERROR":"rgba(249,115,22,.07)","TIMEOUT":"rgba(234,179,8,.06)"}.get(sv,"transparent")
                raw = fc(json.dumps(s.get("raw_data",{}), ensure_ascii=False, default=str))
                step_rows += (
                    "<tr style='background:" + row_bg + "'>"
                      "<td style='display:flex;align-items:center;gap:7px;padding:6px 8px;white-space:nowrap'>"
                        "<span style='width:7px;height:7px;border-radius:50%;background:" + dot_c + ";flex-shrink:0;display:inline-block'></span>"
                        + str(s.get("step","")) + "</td>"
                      "<td style='padding:6px 8px;font-weight:600'>" + fc(str(s.get("name",""))) + "</td>"
                      "<td style='padding:6px 8px;font-weight:800;font-size:11px;color:" + dot_c + "'>" + sv + "</td>"
                      "<td style='padding:6px 8px;color:var(--tx2);max-width:260px'>" + fc(str(s.get("detail",""))[:150]) + "</td>"
                      "<td style='padding:6px 8px'><details><summary style='cursor:pointer;color:var(--info);font-size:11px'>数据</summary><pre style='font-size:10px;white-space:pre-wrap;word-break:break-all;background:var(--bg);padding:6px;border-radius:4px'>" + raw[:2000] + "</pre></details></td>"
                    "</tr>"
                )

            ctx_items = "".join(
                "<div class='ci'><span class='cik'>" + fc(str(k)) + "</span><span class='civ'>" + fc(str(v)[:80]) + "</span></div>"
                for k, v in r.request_context.items()
            )
            http_rows = "".join(
                "<tr>"
                  "<td style='padding:5px 8px;color:var(--info);font-weight:700;font-size:11px'>" + fc(str(h.get("method",""))) + "</td>"
                  "<td style='padding:5px 8px;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap' title='" + fc(str(h.get("url",""))) + "'>" + fc(str(h.get("url",""))) + "</td>"
                  "<td style='padding:5px 8px;font-weight:700'>" + fc(str(h.get("response_status",""))) + "</td>"
                  "<td style='padding:5px 8px;color:var(--tx2);font-size:11px'>" + fc(str(h.get("timestamp",""))) + "</td>"
                "</tr>"
                for h in r.http_calls
            )
            db_rows = "".join(
                "<tr class='" + ("db-hit" if d.get("hit") else "db-miss") + "'>"
                  "<td style='padding:5px 8px;color:var(--info);font-size:11px'>" + fc(str(d.get("db_type",""))) + "</td>"
                  "<td style='padding:5px 8px'>" + fc(str(d.get("action",""))) + "</td>"
                  "<td style='padding:5px 8px;text-align:center;font-size:14px'>" + ("&#x2705;" if d.get("hit") else "&#x274C;") + "</td>"
                  "<td style='padding:5px 8px;color:var(--tx2);font-size:11px'>" + fc(str(d.get("status_flow",""))) + "</td>"
                "</tr>"
                for d in r.db_queries
            )
            es_rows = "".join(
                "<tr>"
                  "<td style='padding:5px 8px;color:var(--tx2);font-size:11px'>" + fc(str(e.get("source",""))) + "</td>"
                  "<td style='padding:5px 8px'><code style='font-size:11px'>" + fc(str(e.get("order_id",""))) + "</code></td>"
                  "<td style='padding:5px 8px'>" + fc(str(e.get("keyword",""))) + "</td>"
                  "<td style='padding:5px 8px;text-align:center'>" + str(e.get("hit_count",0)) + "</td>"
                "</tr>"
                for e in r.es_queries
            )

            ctx_sec = ("<div class='mf-sec'><div class='mf-sec-title'>上下文</div><div class='mf-ctx'>" + (ctx_items or "<span style='color:var(--tx3);font-size:12px'>无</span>") + "</div></div>")
            http_s = ("<div class='mf-sec'><div class='mf-sec-title'>HTTP <span class='mf-badge'>" + str(len(r.http_calls)) + "</span></div><div class='mf-tbl-wrap'><table class='mf-tbl'><thead><tr><th>Method</th><th>URL</th><th>Status</th><th>时间</th></tr></thead><tbody>" + (http_rows or "<tr><td colspan='4' style='color:var(--tx3);font-size:12px'>无</td></tr>") + "</tbody></table></div></div>") if r.http_calls else ""
            db_s = ("<div class='mf-sec'><div class='mf-sec-title'>DB <span class='mf-badge'>" + str(len(r.db_queries)) + "</span></div><div class='mf-tbl-wrap'><table class='mf-tbl'><thead><tr><th>DB</th><th>Action</th><th>命中</th><th>Status</th></tr></thead><tbody>" + (db_rows or "<tr><td colspan='4' style='color:var(--tx3);font-size:12px'>无</td></tr>") + "</tbody></table></div></div>") if r.db_queries else ""
            es_s = ("<div class='mf-sec'><div class='mf-sec-title'>ES <span class='mf-badge'>" + str(len(r.es_queries)) + "</span></div><div class='mf-tbl-wrap'><table class='mf-tbl'><thead><tr><th>Source</th><th>Order ID</th><th>Keyword</th><th>命中</th></tr></thead><tbody>" + (es_rows or "<tr><td colspan='4' style='color:var(--tx3);font-size:12px'>无</td></tr>") + "</tbody></table></div></div>") if r.es_queries else ""

            flow_cards += (
                "\n<div class='mf-flow' id='flow-" + fc(name) + "' style='border-color:" + cfg2["border"] + "'>"
                  "\n  <div class='mf-flow-hd' style='background:rgba(0,0,0,.15);border-bottom:1px solid " + cfg2["border"] + "'>"
                    "\n    <div class='mf-flow-left'>"
                      "\n      <span style='font-size:18px'>" + icon + "</span>"
                      "\n      <span class='mf-fname'>" + fc(name) + "</span>"
                      "\n      <span class='mf-pstatus' style='color:" + cfg2["color"] + ";background:" + cfg2["bg"] + ";border:1px solid " + cfg2["border"] + "'>" + cfg2["label"] + "</span>"
                    "\n    </div>"
                    "\n    <div class='mf-flow-right'>"
                      "\n      <span>HTTP " + str(len(r.http_calls)) + "</span>"
                      "\n      <span>DB " + str(len(r.db_queries)) + "</span>"
                      "\n      <span>ES " + str(len(r.es_queries)) + "</span>"
                      "\n      <span>步骤 " + str(sp) + "/" + str(st) + "</span>"
                    "\n    </div>"
                  "\n  </div>"
                  "\n  <div class='mf-body'>"
                    + ctx_sec + http_s + db_s + es_s +
                    "\n    <div class='mf-sec'>"
                      "\n      <div class='mf-sec-title'>核查步骤 <span class='mf-badge'>" + str(sp) + "/" + str(st) + "</span></div>"
                      "\n      <div class='mf-tbl-wrap'><table class='mf-tbl'><thead><tr><th>#</th><th>名称</th><th>状态</th><th>详情</th><th>数据</th></tr></thead><tbody>" + (step_rows or "<tr><td colspan='5' style='color:var(--tx3);font-size:12px'>无</td></tr>") + "</tbody></table></div>"
                    "\n    </div>"
                  "\n  </div>"
                "\n</div>"
            )

        return (
            "<!DOCTYPE html>"
            "<html lang='zh-CN'>"
            "<head>"
              "<meta charset='UTF-8'>"
              "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
              "<title>REMI 回归测试报告 — " + overall_cfg["label"] + "</title>"
              "<style>"
                "*{box-sizing:border-box;margin:0;padding:0}"
                ":root{--bg:#0f1117;--s:#161a27;--s2:#1e2333;--s3:#252b3d;--bd:#2e3555;--tx:#d8dced;--tx2:#8890a8;--tx3:#4a4d6a;--info:#6366f1;--font:ui-monospace,'Cascadia Code','Fira Code',Menlo,monospace}"
                "body{font-family:var(--font);background:var(--bg);color:var(--tx);font-size:13px;line-height:1.6}"
                ".hdr{background:linear-gradient(135deg,#0d1020,#141728 60%,#181b2e);border-bottom:1px solid var(--bd);padding:18px 24px}"
                ".hdr h1{font-size:15px;font-weight:700;color:#fff;margin-bottom:4px}"
                ".hdr-meta{font-size:11px;color:var(--tx2)}"
                ".hdr-meta span{margin-right:16px}"
                ".mf-alert{margin:16px 24px 0;border:1px solid var(--bd);border-radius:10px;padding:14px 18px;animation:fadeIn .25s ease}"
                "@keyframes fadeIn{from{opacity:0;transform:translateY(-4px)}}"
                ".mf-alert-title{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:#fff;margin-bottom:10px}"
                ".mf-alert-dot{width:8px;height:8px;border-radius:50%;display:inline-block}"
                ".mf-fail-list{display:flex;flex-direction:column;gap:6px}"
                ".mf-fail-item{padding:7px 12px;border-radius:7px;border:1px solid;display:flex;gap:12px;align-items:baseline}"
                ".mf-fail-name{font-weight:700;font-size:12px;min-width:100px;flex-shrink:0}"
                ".mf-fail-reason{font-size:11px;color:var(--tx2)}"
                ".stat-row{display:flex;gap:8px;flex-wrap:wrap;margin:16px 24px 0;background:var(--s);border:1px solid var(--bd);border-radius:10px;padding:12px 16px}"
                ".si{background:var(--s2);border:1px solid var(--bd);border-radius:8px;padding:10px 16px;text-align:center;min-width:70px;flex:1}"
                ".siv{font-size:26px;font-weight:800;color:#fff;line-height:1}"
                ".sil{font-size:9px;color:var(--tx3);text-transform:uppercase;letter-spacing:.8px;margin-top:3px}"
                ".nav{display:flex;gap:8px;flex-wrap:wrap;padding:0 24px;margin:16px 0 12px}"
                ".pill{display:inline-flex;align-items:center;gap:6px;padding:6px 14px;border-radius:20px;font-size:12px;font-weight:600;border:1px solid var(--bd);background:var(--s);cursor:pointer;font-family:var(--font);transition:all .15s}"
                ".content{padding:0 24px 32px}"
                ".mf-flow{background:var(--s);border:1px solid var(--bd);border-radius:12px;margin-bottom:16px;overflow:hidden}"
                ".mf-flow-hd{padding:12px 18px;display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap}"
                ".mf-flow-left{display:flex;align-items:center;gap:10px}"
                ".mf-fname{font-size:15px;font-weight:700;color:#fff}"
                ".mf-pstatus{padding:3px 10px;border-radius:10px;font-size:11px;font-weight:700}"
                ".mf-flow-right{display:flex;gap:14px;font-size:11px;color:var(--tx2)}"
                ".mf-body{padding:0}"
                ".mf-sec{padding:10px 18px;border-bottom:1px solid rgba(46,53,85,.5)}"
                ".mf-sec:last-child{border-bottom:none}"
                ".mf-sec-title{font-size:9px;color:var(--tx3);text-transform:uppercase;letter-spacing:.9px;margin-bottom:8px;display:flex;align-items:center;gap:7px}"
                ".mf-badge{background:var(--s3,#252b3d);color:var(--tx);padding:1px 7px;border-radius:8px;font-size:9px}"
                ".mf-ctx{display:flex;flex-wrap:wrap;gap:6px}"
                ".ci{background:var(--s2);border:1px solid var(--bd);border-radius:6px;padding:4px 10px;display:flex;gap:8px;align-items:baseline;font-size:12px}"
                ".cik{color:var(--tx3);font-size:10px;min-width:70px;flex-shrink:0}"
                ".civ{color:var(--tx);font-size:11px}"
                ".mf-tbl-wrap{overflow-x:auto;border-radius:7px;border:1px solid var(--bd)}"
                ".mf-tbl{width:100%;border-collapse:collapse;font-size:12px;table-layout:auto}"
                ".mf-tbl th{background:var(--s2);padding:7px 9px;text-align:left;font-size:9px;color:var(--tx3);text-transform:uppercase;letter-spacing:.7px;border-bottom:1px solid var(--bd);white-space:nowrap}"
                ".mf-tbl td{padding:6px 9px;border-bottom:1px solid rgba(46,53,85,.4);vertical-align:top}"
                ".mf-tbl tr:last-child td{border-bottom:none}"
                ".mf-tbl tr:hover{background:rgba(255,255,255,.02)}"
                ".db-miss td{opacity:.5}"
              "</style>"
            "</head>"
            "<body>"
              "<div class='hdr'>"
                "<h1>REMI 回归测试报告</h1>"
                "<div class='hdr-meta'>"
                  "<span>⏱ " + fc(self.started_at) + " → " + fc(self.finished_at) + "</span>"
                  "<span>⏺ " + str(int(round(self.duration_seconds,0))) + "s</span>"
                  "<span style='color:" + overall_cfg["color"] + "'>● " + overall_cfg["label"] + " (" + str(passed) + "/" + str(total) + ")</span>"
                "</div>"
              "</div>"
              + top_alert +
              "<div class='stat-row'>" + stat_items + "</div>"
              "<div class='nav'>" + nav_pills + "</div>"
              "<div class='content'>" + flow_cards + "</div>"
            "</body>"
            "</html>"
        )

