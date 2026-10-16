# -*- coding: utf-8 -*-
"""对指定账户跑「正常队列 vs :hi 队列」对照探针。用法: probe_acc.py <account_id>"""
import sys, importlib.util, time, uuid
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
sys.path.insert(0, r"C:\bridge-client\bridge\src")
import redis
from bigqmt_signal_trader.redis_rpc import encode_rpc_request_payload
spec = importlib.util.spec_from_file_location("lc", r"G:\qmt1\python\bigqmt_signal_trader_local_config.py")
lc = importlib.util.module_from_spec(spec); spec.loader.exec_module(lc)
cfg = dict(lc.BIGQMT_REDIS_CONFIG)
r = redis.Redis(host=cfg["host"], port=int(cfg["port"]), db=int(cfg.get("db") or 0),
                password=cfg.get("password") or None, decode_responses=True)
acc = sys.argv[1] if len(sys.argv) > 1 else "666810082889"

def probe(suffix, timeout=5.0):
    rid = uuid.uuid4().hex
    req = {"schema_version": 1, "request_id": rid, "account_id": acc, "method": "ping",
           "params": {}, "ttl_seconds": 60,
           "reply_channel": "bigqmt:rpc:resp:%s:%s" % (acc, rid),
           "reply_list": "bigqmt:rpc:respq:%s:%s" % (acc, rid),
           "reply_key": "bigqmt:rpc:resp:%s:%s" % (acc, rid)}
    q = "bigqmt:rpc:queue:%s%s" % (acc, suffix)
    r.rpush(q, encode_rpc_request_payload(req)); r.expire(q, 300)
    t0 = time.time()
    while time.time() - t0 < timeout:
        if r.get(req["reply_key"]): return True, round(time.time() - t0, 2)
        if r.blpop(req["reply_list"], timeout=1): return True, round(time.time() - t0, 2)
    return False, round(time.time() - t0, 2)

print("账户 = %s" % acc)
ok1, d1 = probe("")
print("  正常队列 ping -> %s (%.2fs)  [阳性对照]" % ("通" if ok1 else "无应答", d1))
ok2, d2 = probe(":hi")
print("  :hi  队列 ping -> %s (%.2fs)  [泵补丁是否已生效]" % ("通" if ok2 else "无应答", d2))
