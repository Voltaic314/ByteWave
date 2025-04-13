import socket
import json
import time

log_entry = {
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "level": "info",
    "message": "This is a test log from Python ✅",
    "details": {
        "source": "test_harness",
        "phase": "init",
        "note": "hello from the Python side"
    }
}

# Encode to JSON and send via UDP
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(json.dumps(log_entry).encode(), ("127.0.0.1", 9999))

print("✅ Sent test log entry to ByteWave")
print("Log entry:", log_entry)
print("Log entry JSON:", json.dumps(log_entry))