import requests
import json
import time
import uuid
import random
import io
import sys
import os
import argparse
from pathlib import Path
from contextlib import redirect_stdout
from typing import Optional, Any

from utils.report_writer import UnifiedReportWriter, MultiFlowReportWriter
from utils.agent_fxa_verifier import AgentFxaVerifier
from utils.agent_fxb_verifier import AgentFxbVerifier
from utils.agent_out_verifier import AgentOutVerifier
from utils.agent_fxc_refund_verifier import AgentFxcRefundVerifier
from utils.agent_fxb_refund_verifier import AgentFxbRefundVerifier
from utils.agent_bison_verifier import AgentBisonVerifier
from utils.agent_out_redeem_verifier import AgentOutRedeemVerifier
from utils.agent_bison_redeem_in_verifier import AgentBisonRedeemInVerifier
from utils.agent_fxb_send_verifier import AgentFxbSendVerifier
from utils.agent_fxa_send_verifier import AgentFxaSendVerifier
from utils.agent_in_send_verifier import AgentInSendVerifier
from utils.agent_fxc_send_verifier import AgentFxcSendVerifier, AgentFxdSendVerifier
from utils.query_db_data import get_latest_read_record

AGENT_URL = "http://remi-agent-engine-fxa-sit.remitech.ai/api/workflow/send-message"
FXB_AGENT_URL = "http://remi-agent-engine-fxb-sit.remitech.ai/api/workflow/send-message"
BISON_AGENT_URL = "http://remi-agent-engine-bison-sit.remitech.ai/api/workflow/send-message"
OUT_AGENT_URL = "http://remi-prep-service-out-sit.remitech.ai/v1/api/prep/sendMessage"
IN_AGENT_URL = "http://remi-prep-service-in-sit.remitech.ai/v1/api/prep/sendMessage"
FXC_AGENT_URL = "http://remi-prep-service-fxc-sit.remitech.ai/v1/api/prep/sendMessage"
FXD_AGENT_URL = "http://remi-prep-service-fxd-sit.remitech.ai/v1/api/prep/sendMessage"
API_KEY = "95e12219871113f500f7e1d7f0f12c5f"
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "summary").strip().lower()
TERMINAL_READ_STATUSES = {"RETURNED_ACK", "RETURNED_NAK", "RETURN_ACK", "RETURN_NAK"}
BLOCKING_READ_STATUSES = {"INIT", "READ"}
PRECHECK_READ_GUARD = os.getenv("PRECHECK_READ_GUARD", "on").strip().lower()

FLOW_DEFS = {
    "FXA": {
        "bics": ("FXTRITSFERA", "FXTREGSFBBB"),
        "db_key": "sit_fxa",
        "role": "不持币成员行",
        "needs_send": True,
        "needs_inward": False,
    },
    "FXA_IN": {
        "bics": ("FXTRITSFERA", "COEBLALALNT"),
        "db_key": "sit_fxa",
        "role": "FXA→IN可持币成员行",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "AGENT_URL",
        "is_agent": True,
    },
    "FXA_OUT": {
        "bics": ("FXTRITSFERA", "AGPBLALAXXX"),
        "db_key": "sit_fxa",
        "role": "FXA→OUT可持币成员行",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "AGENT_URL",
        "is_agent": True,
    },
    "FXA_FXC": {
        "bics": ("FXTRITSFERA", "FXTRITSFERC"),
        "db_key": "sit_fxa",
        "role": "FXA→FXC可持币成员行",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "AGENT_URL",
        "is_agent": True,
    },
    "FXA_FXD": {
        "bics": ("FXTRITSFERA", "FXTRITSFERD"),
        "db_key": "sit_fxa",
        "role": "FXA→FXD可持币成员行",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "AGENT_URL",
        "is_agent": True,
    },
    "FXB": {
        "bics": ("FXTREGSFBBB", "FXTRITSFERA"),
        "db_key": "sit_fxb",
        "role": "不持币成员行",
        "needs_send": False,
        "needs_inward": True,
    },
    "out": {
        "bics": ("AGPBLALAXXX", "COEBLALALNT"),
        "db_key": "sit_out",
        "role": "可持币成员行",
        "needs_send": True,
        "needs_inward": False,
    },
    "in": {
        "bics": ("COEBLALALNT", "AGPBLALAXXX"),
        "db_key": "sit_in",
        "role": "可持币成员行",
        "needs_send": True,
        "needs_inward": False,
    },
    "FXC": {
        "bics": ("FXTRITSFERC", "COEBLALALNT"),
        "db_key": "sit_fxc",
        "role": "可持币成员行(1.2.3)",
        "needs_send": True,
        "needs_inward": False,
    },
    "FXD": {
        "bics": ("FXTRITSFERD", "COEBLALALNT"),
        "db_key": "sit_fxd",
        "role": "可持币成员行(1.2.3)",
        "needs_send": True,
        "needs_inward": False,
    },
    "bison": {
        "bics": ("BNFIPTPLXXX", "FXTRITSFERA"),
        "db_key": "sit_bison",
        "role": "不持币发币行",
        "needs_send": True,
        "needs_inward": False,
    },
    "OUT_REDEEM": {
        "bics": ("AGPBLALAXXX", "BNFIPTPLXXX"),
        "db_key": "sit_out",
        "role": "可持币成员行-非实时赎回",
        "needs_send": False,
        "needs_inward": False,
    },
    "BISON_REDEEM_IN": {
        "bics": ("BNFIPTPLXXX", "FXTREGSFBBB"),
        "db_key": "sit_bison",
        "role": "不持币发币行-收到实时赎回外转+信息币回复",
        "needs_send": False,
        "needs_inward": True,
    },
    "FXC_REFUND": {
        "bics": ("FXTRITSFERC", "FXTREGSFBBB"),
        "db_key": "sit_fxc",
        "role": "可持币成员行(1.2.3)发起退款",
        "needs_send": False,
        "needs_inward": False,
        "is_refund": True,
        "initiator_db": "sit_fxc",
        "target_db": "sit_fxb",
        "sender_remi_id": "FXTREGSFBBB",
    },
    "FXB_REFUND": {
        "bics": ("FXTREGSFBBB", "FXTRITSFERA"),
        "db_key": "sit_fxb",
        "role": "不持币成员行收到退款",
        "needs_send": False,
        "needs_inward": True,
        "is_refund": True,
        "initiator_db": "sit_fxc",
        "target_db": "sit_fxb",
    },
    "FXB_OUT": {
        "bics": ("FXTREGSFBBB", "AGPBLALAXXX"),
        "db_key": "sit_fxb",
        "role": "不持币成员行→可持币成员行",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "FXB_AGENT_URL",
        "is_agent": True,
    },
    "FXB_IN": {
        "bics": ("FXTREGSFBBB", "COEBLALALNT"),
        "db_key": "sit_fxb",
        "role": "不持币成员行→可持币成员行",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "FXB_AGENT_URL",
        "is_agent": True,
    },
    "FXB_FXC": {
        "bics": ("FXTREGSFBBB", "FXTRITSFERC"),
        "db_key": "sit_fxb",
        "role": "不持币成员行→可持币成员行",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "FXB_AGENT_URL",
        "is_agent": True,
    },
    "FXB_FXD": {
        "bics": ("FXTREGSFBBB", "FXTRITSFERD"),
        "db_key": "sit_fxb",
        "role": "不持币成员行→可持币成员行",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "FXB_AGENT_URL",
        "is_agent": True,
    },
    "IN_SEND": {
        "bics": ("COEBLALALNT", "AGPBLALAXXX"),
        "db_key": "sit_in",
        "role": "可持币成员行(IN)发送",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "IN_AGENT_URL",
        "is_agent": False,
    },
    "FXC_SEND": {
        "bics": ("FXTRITSFERC", "AGPBLALAXXX"),
        "db_key": "sit_fxc",
        "role": "可持币成员行(FXC)发送",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "FXC_AGENT_URL",
        "is_agent": False,
    },
    "FXD_SEND": {
        "bics": ("FXTRITSFERD", "AGPBLALAXXX"),
        "db_key": "sit_fxd",
        "role": "可持币成员行(FXD)发送",
        "needs_send": True,
        "needs_inward": False,
        "agent_url": "FXD_AGENT_URL",
        "is_agent": False,
    },
}

_MSG_PREFIX = {"pacs008": "MX008_", "pacs009": "MX009_", "mt103": "MT103_", "mt202": "MT202_"}


def _gen_amount_within_10() -> str:
    return f"{random.randint(1, 100) / 100:.2f}"


class _TeeStream:
    def __init__(self, original, buffer):
        self.original = original
        self.buffer = buffer

    def write(self, data):
        self.original.write(data)
        self.buffer.write(data)
        return len(data)

    def flush(self):
        self.original.flush()
        self.buffer.flush()


def _build_pacs008_xml(instr_id: str, msg_id: str, sender: str, receiver: str,
                       amount: str, current_time: str, current_date: str, uetr: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Saa:DataPDU xmlns:Saa="urn:swift:saa:xsd:saa.2.0" '
        'xmlns:Sw="urn:swift:snl:ns.Sw" xmlns:SwInt="urn:swift:snl:ns.SwInt" '
        'xmlns:SwGbl="urn:swift:snl:ns.SwGbl" xmlns:SwSec="urn:swift:snl:ns.SwSec">'
        '<Saa:Revision>2.0.14</Saa:Revision><Saa:Header><Saa:Message>'
        f'<Saa:SenderReference>{instr_id}</Saa:SenderReference>'
        '<Saa:MessageIdentifier>pacs.008.001.08</Saa:MessageIdentifier>'
        '<Saa:Format>MX</Saa:Format><Saa:SubFormat>Input</Saa:SubFormat>'
        f'<Saa:Sender><Saa:DN>ou=REM,o=SWIFTBIC,o=swift</Saa:DN>'
        f'<Saa:FullName><Saa:X1>{sender}</Saa:X1></Saa:FullName></Saa:Sender>'
        f'<Saa:Receiver><Saa:DN>ou=REM,o=SWIFTBIC,o=swift</Saa:DN>'
        f'<Saa:FullName><Saa:X1>{receiver}</Saa:X1></Saa:FullName></Saa:Receiver>'
        f'</Saa:InterfaceInfo><Saa:UserReference>{instr_id}</Saa:UserReference>'
        '</Saa:InterfaceInfo><Saa:NetworkInfo><Saa:Priority>Normal</Saa:Priority>'
        '<Saa:Service>swift.finplus!pf</Saa:Service>'
        '<Saa:SWIFTNetNetworkInfo><Saa:RequestType>pacs.008.001.08</Saa:RequestType>'
        '<Saa:RequestSubtype>swift.cbprplus.02</Saa:RequestSubtype>'
        '</Saa:SWIFTNetNetworkInfo></Saa:NetworkInfo></Saa:Message></Saa:Header><Saa:Body>'
        '<AppHdr xmlns="urn:iso:std:iso:20022:tech:xsd:head.001.001.02">'
        f'<Fr><FIId><FinInstnId><BICFI>{sender}</BICFI></FinInstnId></FIId></Fr>'
        f'<To><FIId><FinInstnId><BICFI>{receiver}</BICFI></FinInstnId></FIId></To>'
        f'<BizMsgIdr>{instr_id}</BizMsgIdr>'
        '<MsgDefIdr>pacs.008.001.08</MsgDefIdr>'
        '<BizSvc>swift.cbprplus.02</BizSvc>'
        f'<CreDt>{current_time}</CreDt></AppHdr>'
        '<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08">'
        '<FIToFICstmrCdtTrf><GrpHdr><MsgId>' + msg_id + f'</MsgId>'
        f'<CreDtTm>{current_time}</CreDtTm><NbOfTxs>1</NbOfTxs>'
        '<SttlmInf><SttlmMtd>INDA</SttlmMtd></SttlmInf></GrpHdr>'
        '<CdtTrfTxInf><PmtId>'
        f'<InstrId>{instr_id}</InstrId>'
        f'<EndToEndId>{instr_id}</EndToEndId>'
        f'<UETR>{uetr}</UETR></PmtId>'
        f'<IntrBkSttlmAmt Ccy="USD">{amount}</IntrBkSttlmAmt>'
        f'<IntrBkSttlmDt>{current_date}</IntrBkSttlmDt>'
        f'<InstdAmt Ccy="USD">{amount}</InstdAmt>'
        '<ChrgBr>SHAR</ChrgBr>'
        '<ChrgsInf><Amt Ccy="USD">0</Amt>'
        f'<Agt><FinInstnId><BICFI>{sender}</BICFI></FinInstnId></Agt></ChrgsInf>'
        f'<InstgAgt><FinInstnId><BICFI>{sender}</BICFI></FinInstnId></InstgAgt>'
        f'<InstdAgt><FinInstnId><BICFI>{receiver}</BICFI></FinInstnId></InstdAgt>'
        '<Dbtr><Nm>Test User</Nm></Dbtr>'
        '<DbtrAcct><Id><Othr><Id>1234567890</Id></Othr></Id></DbtrAcct>'
        f'<DbtrAgt><FinInstnId><BICFI>{sender}</BICFI></FinInstnId></DbtrAgt>'
        f'<CdtrAgt><FinInstnId><BICFI>{receiver}</BICFI></FinInstnId></CdtrAgt>'
        '<Cdtr><Nm>Agent Account</Nm></Cdtr>'
        '<CdtrAcct><Id><Othr><Id>0987654321</Id></Othr></Id></CdtrAcct>'
        '</CdtTrfTxInf></FIToFICstmrCdtTrf></Document></Saa:Body></Saa:DataPDU>'
    )


def _build_mt103_text(sender: str, receiver: str, reference: str,
                      amount: str, ccy: str) -> str:
    date_yymmdd = time.strftime("%y%m%d")
    seq = str(random.randint(10000000, 99999999))
    blk3_ref = date_yymmdd + seq
    out_pri = sender[:8] + "N" + sender[8:]
    in_pri = receiver[:8] + "N" + receiver[8:]
    return (
        "{1:F01" + out_pri + "0000000000" + "}\n"
        "{2:I103" + in_pri + "N}\n"
        "{3:{108:" + blk3_ref + "}}\n"
        "{4:\n"
        ":20:" + reference + "\n"
        ":23B:CRED\n"
        ":32A:" + date_yymmdd + ccy + amount + "\n"
        ":50K:/0000100110005690\n"
        "LANCER SMITH MR\n"
        "SOPHOUN VILLAGE, MAI DISTRICT,\n"
        "PHONGSALY PROVINCE, LAOS\n"
        ":57A:COEBLALALNT\n"
        ":59:/401000201000003795\n"
        "R5R43RFEW\n"
        ":70:/ROC/\n"
        ":71A:SHA\n"
        ":72:MT103测试MT103测试MT103测试MT103测试MT103测试MT103测试MT103测试\n"
        + sender + "0000000\n" + receiver + "0000000\n"
        "-}}"
    )


def _build_mt202_text(sender: str, receiver: str, reference: str,
                      amount: str, ccy: str) -> str:
    date_yymmdd = time.strftime("%y%m%d")
    seq = str(random.randint(10000000, 99999999))
    blk3_ref = date_yymmdd + seq
    out_pri = sender[:8] + "N" + sender[8:]
    in_pri = receiver[:8] + "N" + receiver[8:]
    return (
        "{1:F01" + out_pri + "0000000018" + "}\n"
        "{2:I202" + in_pri + "N}\n"
        "{3:{108:" + blk3_ref + "}}\n"
        "{4:\n"
        ":20:" + reference + "\n"
        ":21:\n"
        ":32A:" + date_yymmdd + ccy + amount + "\n"
        ":52A:" + sender + "\n"
        ":58A:" + receiver + "\n"
        ":72: MT202测试MT202测试MT202测试MT202测试MT202测试MT202测试MT202测试\n"
        + sender + "0000000\n" + receiver + "0000000\n"
        "-}}"
    )


def _build_pacs009_text(sender: str, receiver: str, reference: str,
                        amount: str, ccy: str, uetr: str) -> str:
    date_yymmdd = time.strftime("%y%m%d")
    seq = str(random.randint(10000000, 99999999))
    blk3_ref = date_yymmdd + seq
    out_pri = sender[:8] + "N" + sender[8:]
    in_pri = receiver[:8] + "N" + receiver[8:]
    current_time = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    return (
        "{1:F01" + out_pri + "0000000018" + "}\n"
        "{2:I202" + in_pri + "N}\n"
        "{3:{108:" + blk3_ref + "}}\n"
        "{4:\n"
        ":20:" + reference + "\n"
        ":21:\n"
        ":32A:" + date_yymmdd + ccy + amount + "\n"
        ":52A:" + sender + "\n"
        ":58A:" + receiver + "\n"
        ":72: MX009测试MX009测试MX009测试MX009测试MX009测试MX009测试MX009测试\n"
        + sender + "0000000\n" + receiver + "0000000\n"
        "-}}"
    )


def _build_message(flow_name: str, msg_type: str, sender: str, receiver: str,
                   instr_id: str, msg_id: str, amount: str, uetr: str) -> str:
    current_time = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    current_date = time.strftime("%Y-%m-%d")
    if msg_type == "pacs008":
        return _build_pacs008_xml(instr_id, msg_id, sender, receiver, amount, current_time, current_date, uetr)
    elif msg_type == "mt103":
        return _build_mt103_text(sender, receiver, instr_id, amount, "USD")
    elif msg_type == "mt202":
        return _build_mt202_text(sender, receiver, instr_id, amount, "USD")
    elif msg_type == "pacs009":
        return _build_pacs009_text(sender, receiver, instr_id, amount, "USD", uetr)
    raise ValueError(f"Unknown msg_type: {msg_type}")


def _preflight_check(db_key: str, mfr: Optional[MultiFlowReportWriter] = None) -> bool:
    guard_on = PRECHECK_READ_GUARD not in {"off", "false", "0", "no"}
    if not guard_on:
        return True
    try:
        latest = get_latest_read_record(db_key)
        if mfr and hasattr(mfr, '_reports'):
            for r in mfr._reports.values():
                if r.request_context.get("direction") == "FXA":
                    r.request_context["latest_read_record_before_send"] = latest
        if not latest:
            print(f"[PRECHECK] read_record 无数据，门禁拦截")
            return False
        status = str(latest.get("status", "")).upper()
        print(f"[PRECHECK] read_record 最后一条状态={status}")
        if status in BLOCKING_READ_STATUSES:
            print(f"[PRECHECK] 状态为 {status}，门禁拦截")
            return False
        if status not in TERMINAL_READ_STATUSES:
            print(f"[PRECHECK] 状态为 {status}，非终态，门禁拦截")
            return False
        print(f"[PRECHECK] read_record 已达终态，前置检查通过")
        return True
    except Exception as e:
        print(f"[PRECHECK] 前置检查跳过（DB查询异常）: {e}")
        return True


def _run_flow_fxa(flow_name: str, msg_type: str, defn: dict,
                   mfr: MultiFlowReportWriter, timeout: int) -> dict:
    sender, receiver = defn["bics"]
    db_key = defn["db_key"]
    prefix = _MSG_PREFIX.get(msg_type, "MX_")
    instr_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(10000, 99999)}"
    uetr = str(uuid.uuid4())
    msg_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(1000000, 9999999)}"
    amount = _gen_amount_within_10()
    current_time = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    message = _build_message(flow_name, msg_type, sender, receiver, instr_id, msg_id, amount, uetr)
    request_id = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10))
    payload = {
        "commonReq": {"apiKey": API_KEY, "clientReqTime": current_time, "requestId": request_id},
        "messageReq": {"message": message},
    }
    report = mfr.get_or_create_report(f"{flow_name}_{msg_type}")
    report.request_context.update({
        "flow": flow_name, "msg_type": msg_type,
        "instr_id": instr_id, "request_id": request_id,
        "amount": amount, "coin_id": "USBT",
        "sender": sender, "receiver": receiver,
        "direction": flow_name,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] 发送 {msg_type}, instr_id={instr_id}, amount={amount}")
    print(f"{'='*60}")
    resp = requests.post(agent_url, json=payload, timeout=30)
    report.add_http_call("send-message", "POST", agent_url,
                         {"Content-Type": "application/json"}, payload,
                         resp.status_code, resp.text)
    print(f"[HTTP] status={resp.status_code}")
    if resp.status_code != 200:
        report.finalize("ERROR", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return
    data = resp.json()
    if data.get("code") != 0:
        report.finalize("ERROR", f"code={data.get('code')}: {data.get('msg','')}")
        return
    order_id = data.get("data")
    report.request_context["order_id"] = order_id
    print(f"[FLOW={flow_name}] 工作流启动成功! order_id={order_id}")
    verifier = AgentFxaVerifier(
        db_key=db_key, bank_req_id=instr_id, order_id=order_id,
        amount=float(amount), coin_id="USBT", report=report,
    )
    try:
        success = verifier.verify_all(timeout=timeout)
    finally:
        verifier.close_conn()
    if isinstance(success, dict):
        is_pass = success.get("status") == "PASS"
    else:
        is_pass = bool(success)
    if is_pass:
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} {msg_type} 验证通过！")
    else:
        failed_steps = [s for s in report.step_results if s["status"] == "FAIL"]
        if failed_steps:
            report.finalize("FAIL", f"Step {failed_steps[0]['step']}: {failed_steps[0]['detail']}")
        else:
            report.finalize("TIMEOUT", "核查步骤未全部通过（可能超时）")
        print(f"\n❌ FLOW={flow_name} {msg_type} 验证失败")


def _run_flow_bison(flow_name: str, msg_type: str, defn: dict,
                    mfr: MultiFlowReportWriter, timeout: int) -> dict:
    sender, receiver = defn["bics"]
    db_key = defn["db_key"]
    prefix = _MSG_PREFIX.get(msg_type, "MX_")
    instr_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(10000, 99999)}"
    uetr = str(uuid.uuid4())
    msg_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(1000000, 9999999)}"
    amount = _gen_amount_within_10()
    current_time = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    message = _build_message(flow_name, msg_type, sender, receiver, instr_id, msg_id, amount, uetr)
    request_id = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10))
    payload = {
        "commonReq": {"apiKey": API_KEY, "clientReqTime": current_time, "requestId": request_id},
        "messageReq": {"message": message},
    }
    report = mfr.get_or_create_report(f"{flow_name}_{msg_type}")
    report.request_context.update({
        "flow": flow_name, "msg_type": msg_type,
        "instr_id": instr_id, "request_id": request_id,
        "amount": amount, "coin_id": "USBT",
        "sender": sender, "receiver": receiver,
        "direction": flow_name,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] 发送 {msg_type}, instr_id={instr_id}, amount={amount}")
    print(f"{'='*60}")
    resp = requests.post(BISON_AGENT_URL, json=payload, timeout=30)
    report.add_http_call("send-message", "POST", BISON_AGENT_URL,
                         {"Content-Type": "application/json"}, payload,
                         resp.status_code, resp.text)
    print(f"[HTTP] status={resp.status_code}")
    if resp.status_code != 200:
        report.finalize("ERROR", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return
    data = resp.json()
    if data.get("code") != 0:
        report.finalize("ERROR", f"code={data.get('code')}: {data.get('msg','')}")
        return
    order_id = data.get("data")
    report.request_context["order_id"] = order_id
    print(f"[FLOW={flow_name}] 工作流启动成功! order_id={order_id}")
    verifier = AgentBisonVerifier(
        db_key=db_key, bank_req_id=instr_id, order_id=order_id,
        amount=float(amount), coin_id="USBT", report=report,
    )
    try:
        success = verifier.verify_all(timeout=timeout)
    finally:
        verifier.close_conn()
    if success:
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} {msg_type} 验证通过！")
    else:
        report.finalize("FAIL", "核查步骤未全部通过")
        print(f"\n❌ FLOW={flow_name} {msg_type} 验证失败")


def _run_flow_fxb(flow_name: str, msg_type: str, defn: dict,
                  mfr: MultiFlowReportWriter, bank_req_id: str, amount: float,
                  coin_id: str, timeout: int):
    db_key = defn["db_key"]
    sender, receiver = defn["bics"]
    report = mfr.get_or_create_report(f"{flow_name}_{msg_type}")
    report.request_context.update({
        "flow": flow_name, "msg_type": msg_type,
        "bank_req_id": bank_req_id,
        "amount": amount, "coin_id": coin_id,
        "direction": flow_name,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] 检查汇入自动赎回, bank_req_id={bank_req_id}")
    print(f"{'='*60}")
    verifier = AgentFxbVerifier(
        db_key=db_key, bank_req_id=bank_req_id,
        amount=amount, coin_id=coin_id, report=report,
    )
    try:
        success = verifier.verify_all(timeout=timeout)
    finally:
        verifier.close_conn()
    if success:
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} {msg_type} 验证通过！")
    else:
        report.finalize("FAIL", "核查步骤未全部通过")
        print(f"\n❌ FLOW={flow_name} {msg_type} 验证失败")


def _run_flow_out(flow_name: str, msg_type: str, defn: dict,
                  mfr: MultiFlowReportWriter, timeout: int):
    sender, receiver = defn["bics"]
    db_key = defn["db_key"]
    prefix = _MSG_PREFIX.get(msg_type, "MX_")
    instr_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(10000, 99999)}"
    uetr = str(uuid.uuid4())
    msg_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(1000000, 9999999)}"
    amount = _gen_amount_within_10()
    current_time = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    message = _build_message(flow_name, msg_type, sender, receiver, instr_id, msg_id, amount, uetr)
    request_id = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10))
    payload = {
        "commonReq": {"apiKey": API_KEY, "clientReqTime": current_time, "requestId": request_id},
        "messageReq": {"message": message},
    }
    report = mfr.get_or_create_report(f"{flow_name}_{msg_type}")
    report.request_context.update({
        "flow": flow_name, "msg_type": msg_type,
        "instr_id": instr_id, "request_id": request_id,
        "amount": amount, "coin_id": "USBT",
        "sender": sender, "receiver": receiver,
        "direction": flow_name,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] 发送 {msg_type}, instr_id={instr_id}, amount={amount}")
    print(f"{'='*60}")
    resp = requests.post(OUT_AGENT_URL, json=payload, timeout=30)
    report.add_http_call("send-message", "POST", OUT_AGENT_URL,
                         {"Content-Type": "application/json"}, payload,
                         resp.status_code, resp.text)
    print(f"[HTTP] status={resp.status_code}")
    if resp.status_code != 200:
        report.finalize("ERROR", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return
    data = resp.json()
    if data.get("code") != 0:
        report.finalize("ERROR", f"code={data.get('code')}: {data.get('msg','')}")
        return
    order_id = data.get("data")
    report.request_context["order_id"] = order_id
    print(f"[FLOW={flow_name}] 工作流启动成功! order_id={order_id}")

    verifier = AgentOutVerifier(
        bank_req_id=instr_id,
        order_id=order_id,
        amount=float(amount),
        coin_id="USBT",
        sender_bic=sender,
        receiver_bic=receiver,
        report=report,
        db_key=db_key,
    )
    try:
        success = verifier.verify_all(timeout=timeout)
    finally:
        verifier.close_conn()
    if isinstance(success, dict):
        is_pass = success.get("status") == "PASS"
    else:
        is_pass = bool(success)
    if is_pass:
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} {msg_type} 验证通过！")
    else:
        failed_steps = [s for s in report.step_results if s["status"] == "FAIL"]
        if failed_steps:
            report.finalize("FAIL", f"Step {failed_steps[0]['step']}: {failed_steps[0]['detail']}")
        else:
            report.finalize("TIMEOUT", "核查步骤未全部通过（可能超时）")
        print(f"\n❌ FLOW={flow_name} {msg_type} 验证失败")


def _run_flow_out_redeem(flow_name: str, defn: dict,
                          mfr: MultiFlowReportWriter,
                          amount: float, coin_id: str, timeout: int):
    redeem_order_id = f"Redeem_{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(1000000, 9999999)}"
    request_id = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=20))
    current_time = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    payload = {
        "commonReq": {
            "apiKey": API_KEY,
            "clientReqTime": current_time,
            "requestId": request_id,
        },
        "redeemReq": {
            "amount": amount,
            "coinType": coin_id,
            "chainType": "solana",
        },
    }
    report = mfr.get_or_create_report(f"{flow_name}_redeem")
    report.request_context.update({
        "flow": flow_name,
        "redeem_order_id": redeem_order_id,
        "amount": amount,
        "coin_id": coin_id,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] 调用 redeemToken, redeem_order_id={redeem_order_id}, amount={amount} {coin_id}")
    print(f"{'='*60}")

    resp = None
    for attempt in range(6):
        try:
            resp = requests.post(OUT_AGENT_URL.replace("/sendMessage", "/redeemToken"), json=payload, timeout=30)
        except Exception as e:
            print(f"[HTTP] 请求异常: {e}")
            if attempt == 5:
                report.finalize("ERROR", f"HTTP 请求失败: {e}")
                return
            time.sleep(15)
            continue

        data = resp.json()
        if data.get("code") == 0:
            print(f"[HTTP] redeemToken status={resp.status_code}, code=0, 成功")
            report.add_http_call("redeemToken", "POST",
                                 OUT_AGENT_URL.replace("/sendMessage", "/redeemToken"),
                                 {"Content-Type": "application/json"}, payload,
                                 resp.status_code, resp.text)
            break
        elif data.get("code") == 50018 and attempt < 5:
            print(f"[HTTP] redeemToken code=50018 busy, 15s后重试 ({attempt+1}/5)")
            time.sleep(15)
            continue
        else:
            print(f"[HTTP] redeemToken code={data.get('code')}, msg={data.get('msg')}")
            report.add_http_call("redeemToken", "POST",
                                 OUT_AGENT_URL.replace("/sendMessage", "/redeemToken"),
                                 {"Content-Type": "application/json"}, payload,
                                 resp.status_code, resp.text)
            report.finalize("ERROR", f"redeemToken code={data.get('code')}: {data.get('msg')}")
            return

    verifier = AgentOutRedeemVerifier(
        amount=amount,
        coin_id=coin_id,
        chain_type="solana",
        report=report,
        db_key="sit_out",
    )
    try:
        result = verifier.verify_redeem_order(redeem_order_id, timeout_s=timeout)
    finally:
        verifier.close_conn()

    if result.get("status") == "PASS":
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} 赎回验证通过！")
    else:
        failed_steps = [s for s in report.step_results if s["status"] == "FAIL"]
        if failed_steps:
            report.finalize("FAIL", f"Step {failed_steps[0]['step']}: {failed_steps[0]['detail']}")
        else:
            report.finalize("TIMEOUT", "核查步骤未全部通过（可能超时）")
        print(f"\n❌ FLOW={flow_name} 赎回验证失败")


def _run_flow_bison_redeem_in(flow_name: str, defn: dict,
                                mfr: MultiFlowReportWriter,
                                burn_tx_id: str, amount: float,
                                coin_id: str, timeout: int):
    report = mfr.get_or_create_report(f"{flow_name}_redeem_in")
    report.request_context.update({
        "flow": flow_name,
        "burn_tx_id": burn_tx_id,
        "amount": amount,
        "coin_id": coin_id,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] 核查 bison 收到实时赎回外转, burn_tx_id={burn_tx_id}, amount={amount} {coin_id}")
    print(f"{'='*60}")

    verifier = AgentBisonRedeemInVerifier(
        burn_tx_id=burn_tx_id,
        amount=amount,
        coin_id=coin_id,
        report=report,
    )
    try:
        result = verifier.verify_all(timeout=timeout)
    finally:
        verifier.close_conn()

    if result.get("status") == "PASS":
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} 验证通过！")
    else:
        failed_steps = [s for s in report.step_results if s["status"] == "FAIL"]
        if failed_steps:
            report.finalize("FAIL", f"Step {failed_steps[0]['step']}: {failed_steps[0]['detail']}")
        else:
            report.finalize("TIMEOUT", "核查步骤未全部通过（可能超时）")
        print(f"\n❌ FLOW={flow_name} 验证失败")


def _run_flow_fxc_refund(flow_name: str, defn: dict,
                          mfr: MultiFlowReportWriter,
                          original_bank_req_id: str,
                          amount: float, coin_id: str,
                          result: int, reject_reason: str,
                          sender_remi_id: str,
                          timeout: int):
    db_key = defn["db_key"]
    report = mfr.get_or_create_report(f"{flow_name}_refund")
    report.request_context.update({
        "flow": flow_name,
        "msg_type": "refund",
        "original_bank_req_id": original_bank_req_id,
        "amount": amount,
        "coin_id": coin_id,
        "result": result,
        "reject_reason": reject_reason,
        "sender_remi_id": sender_remi_id,
        "direction": flow_name,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] FXC发起退款汇入, original_bank_req_id={original_bank_req_id}")
    print(f"{'='*60}")
    verifier = AgentFxcRefundVerifier(
        db_key=db_key,
        bank_req_id=original_bank_req_id,
        amount=amount,
        coin_id=coin_id,
        result=result,
        reject_reason=reject_reason,
        sender_remi_id=sender_remi_id,
        report=report,
        timeout=timeout,
    )
    try:
        success = verifier.verify_all(timeout=timeout)
    finally:
        verifier.close_conn()
    if success:
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} 退款验证通过！")
    else:
        failed_steps = [s for s in report.step_results if s["status"] == "FAIL"]
        if failed_steps:
            report.finalize("FAIL", f"Step {failed_steps[0]['step']}: {failed_steps[0]['detail']}")
        else:
            report.finalize("TIMEOUT", "核查步骤未全部通过（可能超时）")
        print(f"\n❌ FLOW={flow_name} 退款验证失败")


def _run_flow_fxb_refund(flow_name: str, defn: dict,
                         mfr: MultiFlowReportWriter,
                         original_bank_req_id: str,
                         refund_bank_req_id: str,
                         amount: float, coin_id: str,
                         timeout: int):
    db_key = defn["db_key"]
    report = mfr.get_or_create_report(f"{flow_name}_refund")
    report.request_context.update({
        "flow": flow_name,
        "msg_type": "refund",
        "original_bank_req_id": original_bank_req_id,
        "refund_bank_req_id": refund_bank_req_id,
        "amount": amount,
        "coin_id": coin_id,
        "direction": flow_name,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] FXB收到退款汇入, refund_bank_req_id={refund_bank_req_id}")
    print(f"{'='*60}")
    verifier = AgentFxbRefundVerifier(
        db_key=db_key,
        original_bank_req_id=original_bank_req_id,
        refund_bank_req_id=refund_bank_req_id,
        amount=amount,
        coin_id=coin_id,
        report=report,
        timeout=timeout,
    )
    try:
        success = verifier.verify_all(timeout=timeout)
    finally:
        verifier.close_conn()
    if success:
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} 退款汇入验证通过！")
    else:
        failed_steps = [s for s in report.step_results if s["status"] == "FAIL"]
        if failed_steps:
            report.finalize("FAIL", f"Step {failed_steps[0]['step']}: {failed_steps[0]['detail']}")
        else:
            report.finalize("TIMEOUT", "核查步骤未全部通过（可能超时）")
        print(f"\n❌ FLOW={flow_name} 退款汇入验证失败")


def _run_flow_fxa_send(flow_name: str, defn: dict, mfr: Any, timeout: int):
    sender, receiver = defn["bics"]
    db_key = defn["db_key"]
    agent_url_name = defn.get("agent_url", "AGENT_URL")
    agent_url = globals().get(agent_url_name)
    prefix = _MSG_PREFIX.get("pacs008", "MX008_")
    instr_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(10000, 99999)}"
    uetr = str(uuid.uuid4())
    msg_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(1000000, 9999999)}"
    amount = _gen_amount_within_10()
    current_time = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    message = _build_message(flow_name, "pacs008", sender, receiver, instr_id, msg_id, amount, uetr)
    request_id = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10))
    payload = {
        "commonReq": {"apiKey": API_KEY, "clientReqTime": current_time, "requestId": request_id},
        "messageReq": {"message": message},
    }
    report = mfr.get_or_create_report(f"{flow_name}_pacs008")
    report.request_context.update({
        "flow": flow_name, "msg_type": "pacs008",
        "instr_id": instr_id, "request_id": request_id,
        "amount": amount, "coin_id": "USBT",
        "sender": sender, "receiver": receiver,
        "direction": flow_name,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] 发送 pacs008, instr_id={instr_id}, amount={amount}, receiver={receiver}")
    print(f"{'='*60}")
    resp = requests.post(agent_url, json=payload, timeout=30)
    report.add_http_call("send-message", "POST", agent_url,
                         {"Content-Type": "application/json"}, payload,
                         resp.status_code, resp.text)
    print(f"[HTTP] status={resp.status_code}")
    if resp.status_code != 200:
        report.finalize("ERROR", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return
    data = resp.json()
    if data.get("code") != 0:
        report.finalize("ERROR", f"code={data.get('code')}: {data.get('msg','')}")
        return
    order_id = data.get("data")
    report.request_context["order_id"] = order_id
    print(f"[FLOW={flow_name}] 工作流启动成功! order_id={order_id}")

    verifier = AgentFxaSendVerifier(
        bank_req_id=instr_id,
        order_id=order_id,
        amount=float(amount),
        coin_id="USBT",
        sender_bic=sender,
        receiver_bic=receiver,
        report=report,
        db_key=db_key,
    )
    try:
        success = verifier.verify_all(timeout=timeout)
    finally:
        verifier.close_conn()
    if success:
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} pacs008 验证通过！")
    else:
        report.finalize("FAIL", "核查步骤未全部通过")
        print(f"\n❌ FLOW={flow_name} pacs008 验证失败")


def _run_flow_fxb_send(flow_name: str, defn: dict, mfr: Any, timeout: int):
    sender, receiver = defn["bics"]
    db_key = defn["db_key"]
    agent_url_name = defn.get("agent_url", "FXB_AGENT_URL")
    agent_url = globals().get(agent_url_name)
    prefix = _MSG_PREFIX.get("pacs008", "MX008_")
    instr_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(10000, 99999)}"
    uetr = str(uuid.uuid4())
    msg_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(1000000, 9999999)}"
    amount = _gen_amount_within_10()
    current_time = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    message = _build_message(flow_name, "pacs008", sender, receiver, instr_id, msg_id, amount, uetr)
    request_id = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10))
    payload = {
        "commonReq": {"apiKey": API_KEY, "clientReqTime": current_time, "requestId": request_id},
        "messageReq": {"message": message},
    }
    report = mfr.get_or_create_report(f"{flow_name}_pacs008")
    report.request_context.update({
        "flow": flow_name, "msg_type": "pacs008",
        "instr_id": instr_id, "request_id": request_id,
        "amount": amount, "coin_id": "USBT",
        "sender": sender, "receiver": receiver,
        "direction": flow_name,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] 发送 pacs008, instr_id={instr_id}, amount={amount}, receiver={receiver}")
    print(f"{'='*60}")
    resp = requests.post(AGENT_URL, json=payload, timeout=30)
    report.add_http_call("send-message", "POST", AGENT_URL,
                         {"Content-Type": "application/json"}, payload,
                         resp.status_code, resp.text)
    print(f"[HTTP] status={resp.status_code}")
    if resp.status_code != 200:
        report.finalize("ERROR", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return
    data = resp.json()
    if data.get("code") != 0:
        report.finalize("ERROR", f"code={data.get('code')}: {data.get('msg','')}")
        return
    order_id = data.get("data")
    report.request_context["order_id"] = order_id
    print(f"[FLOW={flow_name}] 工作流启动成功! order_id={order_id}")

    verifier = AgentFxbSendVerifier(
        bank_req_id=instr_id,
        order_id=order_id,
        amount=float(amount),
        coin_id="USBT",
        sender_bic=sender,
        receiver_bic=receiver,
        report=report,
        db_key=db_key,
    )
    try:
        success = verifier.verify_all(timeout=timeout)
    finally:
        verifier.close_conn()
    if success:
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} pacs008 验证通过！")
    else:
        report.finalize("FAIL", "核查步骤未全部通过")
        print(f"\n❌ FLOW={flow_name} pacs008 验证失败")


def _run_flow_core_send(flow_name: str, defn: dict, mfr: Any, timeout: int):
    sender, receiver = defn["bics"]
    db_key = defn["db_key"]
    agent_url_name = defn["agent_url"]
    agent_url = globals().get(agent_url_name)
    prefix = _MSG_PREFIX.get("pacs008", "MX008_")
    instr_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(10000, 99999)}"
    uetr = str(uuid.uuid4())
    msg_id = f"{prefix}{time.strftime('%Y%m%d-%H%M%S')}-{random.randint(1000000, 9999999)}"
    amount = _gen_amount_within_10()
    current_time = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    message = _build_message(flow_name, "pacs008", sender, receiver, instr_id, msg_id, amount, uetr)
    request_id = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10))
    payload = {
        "commonReq": {"apiKey": API_KEY, "clientReqTime": current_time, "requestId": request_id},
        "messageReq": {"message": message},
    }
    report = mfr.get_or_create_report(f"{flow_name}_pacs008")
    report.request_context.update({
        "flow": flow_name, "msg_type": "pacs008",
        "instr_id": instr_id, "request_id": request_id,
        "amount": amount, "coin_id": "USBT",
        "sender": sender, "receiver": receiver,
        "direction": flow_name,
    })
    print(f"\n{'='*60}")
    print(f"[FLOW={flow_name}] 发送 pacs008, instr_id={instr_id}, amount={amount}, receiver={receiver}")
    print(f"{'='*60}")
    resp = requests.post(agent_url, json=payload, timeout=30)
    report.add_http_call("send-message", "POST", agent_url,
                         {"Content-Type": "application/json"}, payload,
                         resp.status_code, resp.text)
    print(f"[HTTP] status={resp.status_code}")
    if resp.status_code != 200:
        report.finalize("ERROR", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return
    data = resp.json()
    if data.get("code") != 0:
        report.finalize("ERROR", f"code={data.get('code')}: {data.get('msg','')}")
        return
    order_id = data.get("data")
    report.request_context["order_id"] = order_id
    print(f"[FLOW={flow_name}] 工作流启动成功! order_id={order_id}")

    if flow_name in ("FXC_SEND",):
        verifier = AgentFxcSendVerifier(
            bank_req_id=instr_id,
            order_id=order_id,
            amount=float(amount),
            coin_id="USBT",
            sender_bic=sender,
            receiver_bic=receiver,
            report=report,
            db_key=db_key,
        )
    elif flow_name in ("FXD_SEND",):
        verifier = AgentFxdSendVerifier(
            bank_req_id=instr_id,
            order_id=order_id,
            amount=float(amount),
            coin_id="USBT",
            sender_bic=sender,
            receiver_bic=receiver,
            report=report,
        )
    else:
        verifier = AgentInSendVerifier(
            bank_req_id=instr_id,
            order_id=order_id,
            amount=float(amount),
            coin_id="USBT",
            sender_bic=sender,
            receiver_bic=receiver,
            report=report,
            db_key=db_key,
        )
    try:
        success = verifier.verify_all(timeout=timeout)
    finally:
        verifier.close_conn()
    if success:
        report.finalize("PASS", "")
        print(f"\n🎉 FLOW={flow_name} pacs008 验证通过！")
    else:
        report.finalize("FAIL", "核查步骤未全部通过")
        print(f"\n❌ FLOW={flow_name} pacs008 验证失败")


def main():
    parser = argparse.ArgumentParser(description="REMI Agent 全链路自动化回归测试（多流程版）")
    parser.add_argument("--flows", dest="flows", default="FXA",
                        help="要执行的流程，逗号分隔，如 FXA,FXB,out,FXC,FXD,bison（默认 FXA）")
    parser.add_argument("--msg-types", dest="msg_types", default="pacs008",
                        help="报文格式，逗号分隔，如 pacs008,mt103（默认 pacs008）")
    parser.add_argument("--full", action="store_true",
                        help="使用全部4种报文格式（pacs008,pacs009,mt103,mt202）")
    parser.add_argument("--direction", choices=["FXA", "FXB", "BOTH"], default=None,
                        help="（兼容旧接口）")
    parser.add_argument("--bank-req-id", dest="bank_req_id", default="",
                        help="FXB 专用：指定 bank_req_id")
    parser.add_argument("--amount", dest="amount", default="0.2",
                        help="汇入金额（FXB）")
    parser.add_argument("--coin-id", dest="coin_id", default="USBT",
                        help="币种（FXB）")
    parser.add_argument("--refund-original-bank-req-id", dest="refund_original_bank_req_id", default="",
                        help="退款流程专用：原始汇入的 bank_req_id")
    parser.add_argument("--refund-result", dest="refund_result", type=int, default=1,
                        help="退款结果：1=同意退, 0=拒绝")
    parser.add_argument("--refund-reason", dest="refund_reason", default="high risk",
                        help="退款原因")
    parser.add_argument("--sender-remi-id", dest="sender_remi_id", default="FXTREGSFBBB",
                        help="发起退款的成员行 BIC")
    parser.add_argument("--burn-tx-id", dest="burn_tx_id", default="",
                        help="BISON 实时赎回的 burn tx_id")
    parser.add_argument("--timeout", dest="timeout", type=int, default=60,
                        help="每个flow的验证超时秒数（默认60s）")
    parser.add_argument("--name", dest="run_name", default="",
                        help="本次运行的命名标识")
    args = parser.parse_args()

    if args.full:
        msg_types = ["pacs008", "pacs009", "mt103", "mt202"]
    else:
        msg_types = [m.strip() for m in args.msg_types.split(",")]

    flow_names = [f.strip() for f in args.flows.split(",")]
    run_name = args.run_name or ("_".join(flow_names))
    run_id = str(uuid.uuid4())[:8]
    timestamp = time.strftime("%Y%m%d_%H%M%S")

    mfr = MultiFlowReportWriter(
        run_id=run_id, run_name=run_name,
        flows=flow_names, msg_types=msg_types,
        debug_level=DEBUG_LEVEL,
    )
    flow_defs = {k: v for k, v in FLOW_DEFS.items() if k in flow_names}

    print(f"当前 DEBUG_LEVEL={DEBUG_LEVEL}")
    print(f"执行流程: {flow_names}")
    print(f"报文格式: {msg_types}")
    print(f"每flow超时: {args.timeout}s")

    buf = io.StringIO()
    tee = _TeeStream(sys.stdout, buf)

    with redirect_stdout(tee):
        try:
            if args.direction in ("FXB", "BOTH") and args.bank_req_id:
                bdef = FLOW_DEFS.get("FXB", {})
                _run_flow_fxb("FXB", "pacs008", bdef, mfr,
                              args.bank_req_id, float(args.amount),
                              args.coin_id, args.timeout)

            for flow_name in flow_names:
                defn = FLOW_DEFS.get(flow_name)
                if not defn:
                    print(f"⚠️ 未知流程: {flow_name}，跳过")
                    continue
                for mt in msg_types:
                    try:
                        if flow_name == "FXA":
                            _run_flow_fxa(flow_name, mt, defn, mfr, args.timeout)
                        elif flow_name == "FXB":
                            if args.bank_req_id:
                                _run_flow_fxb(flow_name, mt, defn, mfr,
                                              args.bank_req_id, float(args.amount),
                                              args.coin_id, args.timeout)
                            else:
                                print(f"⚠️ FXB 需要 --bank-req-id，跳过")
                        elif flow_name == "out":
                            _run_flow_out(flow_name, mt, defn, mfr, args.timeout)
                        elif flow_name == "bison":
                            _run_flow_bison(flow_name, mt, defn, mfr, args.timeout)
                        elif flow_name == "OUT_REDEEM":
                            if not args.amount:
                                print(f"⚠️ OUT_REDEEM 需要 --amount 参数，跳过")
                            else:
                                _run_flow_out_redeem(
                                    flow_name, defn, mfr,
                                    amount=float(args.amount),
                                    coin_id=args.coin_id or "USBT",
                                    timeout=args.timeout,
                                )
                        elif flow_name == "BISON_REDEEM_IN":
                            if not getattr(args, "burn_tx_id", None):
                                print(f"⚠️ BISON_REDEEM_IN 需要 --burn-tx-id 参数，跳过")
                            else:
                                _run_flow_bison_redeem_in(
                                    flow_name, defn, mfr,
                                    burn_tx_id=args.burn_tx_id,
                                    amount=float(args.amount) if args.amount else 0,
                                    coin_id=args.coin_id or "USBT",
                                    timeout=args.timeout,
                                )
                        elif flow_name == "FXC_REFUND":
                            if not args.refund_original_bank_req_id:
                                print(f"⚠️ FXC_REFUND 需要 --refund-original-bank-req-id，跳过")
                            else:
                                _run_flow_fxc_refund(
                                    flow_name, defn, mfr,
                                    original_bank_req_id=args.refund_original_bank_req_id,
                                    amount=float(args.amount),
                                    coin_id=args.coin_id,
                                    result=args.refund_result,
                                    reject_reason=args.refund_reason,
                                    sender_remi_id=args.sender_remi_id,
                                    timeout=args.timeout,
                                )
                                refund_bank_req = args.refund_original_bank_req_id + "_R1"
                                _run_flow_fxb_refund(
                                    "FXB_REFUND",
                                    FLOW_DEFS["FXB_REFUND"],
                                    mfr,
                                    original_bank_req_id=args.refund_original_bank_req_id,
                                    refund_bank_req_id=refund_bank_req,
                                    amount=float(args.amount),
                                    coin_id=args.coin_id,
                                    timeout=args.timeout,
                                )
                        elif flow_name == "FXB_REFUND":
                            if not args.refund_original_bank_req_id:
                                print(f"⚠️ FXB_REFUND 需要 --refund-original-bank-req-id，跳过")
                            else:
                                _run_flow_fxb_refund(
                                    flow_name, defn, mfr,
                                    original_bank_req_id=args.refund_original_bank_req_id,
                                    refund_bank_req_id=args.refund_original_bank_req_id + "_R1",
                                    amount=float(args.amount),
                                    coin_id=args.coin_id,
                                    timeout=args.timeout,
                                )
                        elif flow_name in ("FXA_IN", "FXA_OUT", "FXA_FXC", "FXA_FXD"):
                            _run_flow_fxa_send(flow_name, defn, mfr, args.timeout)
                        elif flow_name in ("FXB_OUT", "FXB_IN", "FXB_FXC", "FXB_FXD"):
                            _run_flow_fxb_send(flow_name, defn, mfr, args.timeout)
                        elif flow_name in ("IN_SEND", "FXC_SEND", "FXD_SEND"):
                            _run_flow_core_send(flow_name, defn, mfr, args.timeout)
                        else:
                            print(f"⚠️ {flow_name} 暂未实现验证器，跳过（DB查询待接入）")
                    except Exception as e:
                        rep = mfr.get_or_create_report(f"{flow_name}_{mt}")
                        rep.finalize("ERROR", str(e))
                        print(f"❌ FLOW={flow_name} {mt} 执行异常: {e}")
        finally:
            mfr.finalize_all()
            mfr.console_logs = buf.getvalue()

    json_path, html_path = mfr.write(suffix=timestamp)
    print(f"\n📄 报告(JSON): {json_path}")
    print(f"📄 报告(HTML): {html_path}")


if __name__ == "__main__":
    main()
