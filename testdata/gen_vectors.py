#!/usr/bin/env python3
"""Golden vectors for GoLoqed, mirroring loqedAPI 2.1.16 (src/loqedAPI/loqed.py).

Run: python3 testdata/gen_vectors.py
Paste the output into bridge/*_test.go when the protocol changes.
"""
import base64, hashlib, hmac, struct, urllib.parse

SECRET = "SGFsbG8gd2VyZWxk"          # key_secret
BRIDGE_KEY = "Ym9uam91ciBtb25kZQ=="  # bridge_key
KEY_ID = 1
NOW = 1700000000
K = base64.b64decode(BRIDGE_KEY)

def command(action):
    signed = struct.pack("B", 2) + struct.pack("B", 7) + NOW.to_bytes(8, "big") \
        + struct.pack("B", KEY_ID) + struct.pack("B", 1) + struct.pack("B", action)
    hm = hmac.new(base64.b64decode(SECRET), signed, hashlib.sha256).digest()
    cmd = struct.pack("Q", 0) + struct.pack("B", 2) + struct.pack("B", 7) + NOW.to_bytes(8, "big") \
        + hm + struct.pack("B", KEY_ID) + struct.pack("B", 1) + struct.pack("B", action)
    return urllib.parse.quote(base64.b64encode(cmd).decode("ascii"))

for action in (1, 2, 3):
    print(f"command action={action}: {command(action)}")
print("list webhooks:", hashlib.sha256(NOW.to_bytes(8, "big") + K).hexdigest())
url = "http://10.0.0.5:8099/webhook/lock1"
print("create webhook:", hashlib.sha256(url.encode() + (511).to_bytes(4, "big") + NOW.to_bytes(8, "big") + K).hexdigest())
print("delete webhook id=7:", hashlib.sha256((7).to_bytes(8, "big") + NOW.to_bytes(8, "big") + K).hexdigest())
body = '{"requested_state":"NIGHT_LOCK","requested_state_numeric":3,"mac_wifi":"aa","mac_ble":"bb","event_type":"STATE_CHANGED_NIGHT_LOCK","key_local_id":255}'
print("event:", hashlib.sha256(body.encode() + NOW.to_bytes(8, "big") + K).hexdigest())
