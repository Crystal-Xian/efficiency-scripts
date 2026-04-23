import os
import re
import time
import json
from typing import Any, Dict, List, Optional, Tuple

import requests

_DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "summary").strip().lower()
_DETAIL = _DEBUG_LEVEL == "detail"


def _extract_fail_reason(message: str) -> Optional[str]:
    patterns = [
        r'"failReason"\s*:\s*"([^"]+)"',
        r"'failReason'\s*:\s*'([^']+)'",
        r"failReason\s*[:=]\s*([^,}\]]+)",
    ]
    for p in patterns:
        m = re.search(p, message)
        if m:
            return m.group(1).strip().strip('"').strip("'")
    return None


class EsLogChecker:
    """
    ES/Kibana 日志轮询检查器：
    - 优先走 ES _search 直连
    - 失败时回退 Kibana Console Proxy
    """

    def __init__(
        self,
        es_search_url: Optional[str] = None,
        kibana_base_url: Optional[str] = None,
        index_pattern: str = "logs-*",
        cluster_source: str = "sit",
        username: Optional[str] = None,
        password: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        self.es_search_url = es_search_url or os.getenv("ES_SEARCH_URL", "").strip()
        self.kibana_base_url = (kibana_base_url or os.getenv("KIBANA_BASE_URL", "")).rstrip("/")
        self.index_pattern = index_pattern
        self.cluster_source = cluster_source
        self.username = username or os.getenv("ES_USERNAME")
        self.password = password or os.getenv("ES_PASSWORD")
        self.api_key = api_key or os.getenv("ES_API_KEY")

    def _auth_headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"ApiKey {self.api_key}"
        return headers

    def _request_kwargs(self) -> Dict[str, Any]:
        if self.api_key:
            return {}
        if self.username and self.password:
            return {"auth": (self.username, self.password)}
        return {}

    def _build_query(self, order_id: str, api_keyword: str, time_from: str, size: int) -> Dict[str, Any]:
        return {
            "size": size,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"message": order_id}},
                        {"match": {"message": api_keyword}},
                    ],
                    "filter": [
                        {"range": {"@timestamp": {"gte": time_from, "lte": "now"}}},
                    ],
                }
            },
        }

    def _search_via_es(self, payload: Dict[str, Any], timeout_s: int) -> Dict[str, Any]:
        if not self.es_search_url:
            raise RuntimeError("未配置 ES_SEARCH_URL")
        headers = self._auth_headers()
        print(f"[HTTP][es_search] POST {self.es_search_url}")
        if _DETAIL:
            print(f"[HTTP][es_search] request_headers={json.dumps(headers, ensure_ascii=False)}")
            print(f"[HTTP][es_search] request_body={json.dumps(payload, ensure_ascii=False)}")
        resp = requests.post(
            self.es_search_url,
            json=payload,
            headers=headers,
            timeout=timeout_s,
            **self._request_kwargs(),
        )
        print(f"[HTTP][es_search] response_status={resp.status_code}")
        if _DETAIL:
            print(f"[HTTP][es_search] response_body={resp.text[:2000]}")
        resp.raise_for_status()
        return resp.json()

    def _search_via_kibana_proxy(self, payload: Dict[str, Any], timeout_s: int) -> Dict[str, Any]:
        if not self.kibana_base_url:
            raise RuntimeError("未配置 KIBANA_BASE_URL")
        proxy_url = (
            f"{self.kibana_base_url}/api/console/proxy"
            f"?path=/{self.index_pattern}/_search&method=POST"
        )
        headers = self._auth_headers()
        headers["kbn-xsrf"] = "true"
        print(f"[HTTP][kibana_proxy_search] POST {proxy_url}")
        if _DETAIL:
            print(f"[HTTP][kibana_proxy_search] request_headers={json.dumps(headers, ensure_ascii=False)}")
            print(f"[HTTP][kibana_proxy_search] request_body={json.dumps(payload, ensure_ascii=False)}")
        resp = requests.post(
            proxy_url,
            json=payload,
            headers=headers,
            timeout=timeout_s,
            **self._request_kwargs(),
        )
        print(f"[HTTP][kibana_proxy_search] response_status={resp.status_code}")
        if _DETAIL:
            print(f"[HTTP][kibana_proxy_search] response_body={resp.text[:2000]}")
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _extract_hits(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return data.get("hits", {}).get("hits", []) or []

    def search_once(
        self,
        order_id: str,
        api_keyword: str = "updateOnchainStatus",
        time_from: str = "now-30m",
        size: int = 200,
        timeout_s: int = 15,
    ) -> Tuple[List[Dict[str, Any]], str]:
        payload = self._build_query(order_id, api_keyword, time_from, size)
        print("[ES] SEARCH START")
        print(f"[ES] order_id={order_id}, api_keyword={api_keyword}, time_from={time_from}, size={size}")
        if _DETAIL:
            print(f"[ES] query_payload={json.dumps(payload, ensure_ascii=False)}")

        last_error = None
        try:
            data = self._search_via_es(payload, timeout_s)
            hits = self._extract_hits(data)
            print(f"[ES] source=es, hit_count={len(hits)}")
            if _DETAIL and hits:
                print(f"[ES] first_hit={json.dumps(hits[0], ensure_ascii=False, default=str)}")
            return hits, "es"
        except Exception as e:
            last_error = str(e)
            print(f"[ES] source=es failed: {last_error}")

        try:
            data = self._search_via_kibana_proxy(payload, timeout_s)
            hits = self._extract_hits(data)
            print(f"[ES] source=kibana_proxy, hit_count={len(hits)}")
            if _DETAIL and hits:
                print(f"[ES] first_hit={json.dumps(hits[0], ensure_ascii=False, default=str)}")
            return hits, "kibana_proxy"
        except Exception as e:
            raise RuntimeError(f"ES 与 Kibana 查询均失败: es_err={last_error}; kibana_err={e}")

    def poll_callback_log(
        self,
        order_id: str,
        api_keyword: str = "updateOnchainStatus",
        expect_onchain_status: Optional[str] = None,
        require_fail_reason: bool = False,
        time_from: str = "now-30m",
        timeout_s: int = 300,
        interval_s: int = 10,
        size: int = 200,
    ) -> Dict[str, Any]:
        deadline = time.time() + timeout_s
        last_err = None

        while time.time() < deadline:
            print("[ES] POLL ROUND ...")
            try:
                hits, source = self.search_once(
                    order_id=order_id,
                    api_keyword=api_keyword,
                    time_from=time_from,
                    size=size,
                )
                if hits:
                    parsed = []
                    matched = []
                    for h in hits:
                        src = h.get("_source", {}) or {}
                        msg = str(src.get("message", ""))
                        reason = _extract_fail_reason(msg)
                        parsed_item = {
                            "timestamp": src.get("@timestamp"),
                            "message": msg,
                            "fail_reason": reason,
                            "pod_name": src.get("pod_name"),
                            "log_file_path": src.get("log", {}).get("file", {}).get("path")
                            if isinstance(src.get("log"), dict)
                            else None,
                        }
                        parsed.append(parsed_item)
                        if expect_onchain_status:
                            if re.search(
                                re.compile(
                                    r'"?[Oo]n[Cc]hain[Ss]tatus"?\s*:\s*"?'
                                    + re.escape(expect_onchain_status),
                                    re.IGNORECASE,
                                ),
                                msg,
                            ):
                                matched.append(parsed_item)
                        else:
                            matched.append(parsed_item)

                    if matched:
                        has_fail_reason = any(x.get("fail_reason") for x in matched)
                        if require_fail_reason and not has_fail_reason:
                            return {
                                "status": "FAIL",
                                "query_source": source,
                                "order_id": order_id,
                                "api_keyword": api_keyword,
                                "hit_count": len(matched),
                                "latest_fail_reason": None,
                                "detail": "命中回调日志，但未解析到 failReason",
                                "hits": matched,
                            }
                        return {
                            "status": "PASS",
                            "query_source": source,
                            "order_id": order_id,
                            "api_keyword": api_keyword,
                            "hit_count": len(matched),
                            "latest_fail_reason": matched[0].get("fail_reason"),
                            "hits": matched,
                        }
            except Exception as e:
                last_err = str(e)

            time.sleep(interval_s)

        return {
            "status": "TIMEOUT",
            "order_id": order_id,
            "api_keyword": api_keyword,
            "expected_onchain_status": expect_onchain_status,
            "error": last_err,
            "detail": "轮询超时，未匹配到回调日志",
        }
