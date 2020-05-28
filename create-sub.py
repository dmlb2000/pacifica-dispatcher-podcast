#!/usr/bin/python
import os
import requests

notify_url = os.getenv('NOTIFY_URL', 'http://notifyfrontend:8070')
self_url = os.getenv('SELF_URL', 'http://jupyter:8080')
remote_user = os.getenv('REMOTE_USER', 'dmlb2001')
resp = requests.post(
    '{}/eventmatch'.format(notify_url),
    headers={'Http-Remote-User': remote_user},
    json={
        "name": "My Event Match",
        "jsonpath": """
            $[?(
                @["cloudEventsVersion"] = "0.1" and
                @["eventType"] = "org.pacifica.metadata.ingest"
        )]
        """,
        "target_url": "{}/receive".format(self_url)
    }
)
assert resp.status_code == 200
print(resp.json())
subscription_uuid = resp.json()['uuid']