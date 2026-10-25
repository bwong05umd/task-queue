#!/usr/bin/env python3
"""Submit sample jobs: python3 scripts/submit.py [count] [type]

API_URL (default http://localhost:8000) and API_KEY select the target."""

import json
import os
import random
import sys
import urllib.request

SAMPLES = {
    "email.send": lambda: {"to": f"user{random.randint(1, 999)}@example.com", "subject": "hi"},
    "image.resize": lambda: {"width": 1920, "height": 1080, "target_width": random.choice([320, 640, 1280])},
    "sleep": lambda: {"seconds": round(random.uniform(0.1, 2), 2)},
    "flaky": lambda: {"fail_times": random.randint(0, 3)},
    "poison": lambda: {},
}

count = int(sys.argv[1]) if len(sys.argv) > 1 else 20
kinds = [sys.argv[2]] if len(sys.argv) > 2 else ["email.send", "image.resize", "sleep", "flaky"]
for _ in range(count):
    kind = random.choice(kinds)
    req = urllib.request.Request(
        os.environ.get("API_URL", "http://localhost:8000") + "/jobs",
        method="POST",
        data=json.dumps({"type": kind, "payload": SAMPLES[kind]()}).encode(),
        headers={"Content-Type": "application/json", **({"X-API-Key": k} if (k := os.environ.get("API_KEY")) else {})},
    )
    with urllib.request.urlopen(req) as r:
        job = json.load(r)
    print(job["id"], kind)
