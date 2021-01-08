"""Group output of gtest by thread id

Example usage:

> python3 tools/deinterleave.py <<EOF  
I1226 06:33:06.426666 11224 broker.cpp:113] Bound Broker to: ipc:///tmp/test_e2e0
I1226 06:33:06.427703 11224 broker.cpp:133] Sent READY message to ipc:///tmp/test_e2e0
I1226 06:33:06.447345 11232 broker.cpp:113] Bound Broker to: ipc:///tmp/test_e2e1
I1226 06:33:06.447707 11232 broker.cpp:133] Sent READY message to ipc:///tmp/test_e2e0
I1226 06:33:06.447993 11232 broker.cpp:133] Sent READY message to ipc:///tmp/test_e2e1
I1226 06:33:06.448770 11232 broker.cpp:146] Waiting for READY messages from other machines...
I1226 06:33:06.448783 11232 broker.cpp:172] Received READY message from /tmp/test_e2e1 (rep: 0, part: 1)
I1226 06:33:06.449044 11232 broker.cpp:172] Received READY message from /tmp/test_e2e0 (rep: 0, part: 0)
I1226 06:33:06.449077 11224 broker.cpp:133] Sent READY message to ipc:///tmp/test_e2e1
I1226 06:33:06.449609 11224 broker.cpp:146] Waiting for READY messages from other machines...
I1226 06:33:06.449635 11224 broker.cpp:172] Received READY message from /tmp/test_e2e0 (rep: 0, part: 0)
I1226 06:33:06.449654 11224 broker.cpp:172] Received READY message from /tmp/test_e2e1 (rep: 0, part: 1)
I1226 06:33:06.450145 11232 broker.cpp:181] All READY messages received
I1226 06:33:06.450448 11224 broker.cpp:181] All READY messages received
EOF

========================================================================================================
Thread "11224"
I1226 06:33:06.426666 11224 broker.cpp:113] Bound Broker to: ipc:///tmp/test_e2e0
I1226 06:33:06.427703 11224 broker.cpp:133] Sent READY message to ipc:///tmp/test_e2e0
I1226 06:33:06.449077 11224 broker.cpp:133] Sent READY message to ipc:///tmp/test_e2e1
I1226 06:33:06.449609 11224 broker.cpp:146] Waiting for READY messages from other machines...
I1226 06:33:06.449635 11224 broker.cpp:172] Received READY message from /tmp/test_e2e0 (rep: 0, part: 0)
I1226 06:33:06.449654 11224 broker.cpp:172] Received READY message from /tmp/test_e2e1 (rep: 0, part: 1)
I1226 06:33:06.450448 11224 broker.cpp:181] All READY messages received

Thread "11232"
I1226 06:33:06.447345 11232 broker.cpp:113] Bound Broker to: ipc:///tmp/test_e2e1
I1226 06:33:06.447707 11232 broker.cpp:133] Sent READY message to ipc:///tmp/test_e2e0
I1226 06:33:06.447993 11232 broker.cpp:133] Sent READY message to ipc:///tmp/test_e2e1
I1226 06:33:06.448770 11232 broker.cpp:146] Waiting for READY messages from other machines...
I1226 06:33:06.448783 11232 broker.cpp:172] Received READY message from /tmp/test_e2e1 (rep: 0, part: 1)
I1226 06:33:06.449044 11232 broker.cpp:172] Received READY message from /tmp/test_e2e0 (rep: 0, part: 0)
I1226 06:33:06.450145 11232 broker.cpp:181] All READY messages received
"""
import re
import sys

logs_per_thread = {}
max_len = 10
for line in sys.stdin:
    log = line.strip()
    max_len = max(max_len, len(log))
    tokens = re.split('\s+', log)
    if len(tokens) > 3:
        thread_id = tokens[2]
        logs_per_thread.setdefault(thread_id, []).append(log)

print()
print('='*max_len)
for thread_id in logs_per_thread:
    print(f'Thread "{thread_id}"')
    for log in logs_per_thread[thread_id]:
        print(log)
    print()

