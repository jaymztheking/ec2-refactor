import os
import requests
import json

api_urls = {}
api_urls['dev'] = {}
api_urls['dev']['full_load'] = 'https://hooks.slack.com/services/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'
api_urls['dev']['incremental_load'] = 'https://hooks.slack.com/services/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'

template_path = os.path.join('.','ingestion','slack_message_template.json')

class SlackSender:

    def __init__(self, env: str, load_type: str):
        with open(template_path) as f:
            self.message_template = json.load(f)
        self.api_url = api_urls[env][load_type]

    def send_message(self, message: dict) -> bool:
        r = requests.post(self.api_url, json=message)
        return r.status_code == 200

test = SlackSender('dev', 'full_load')
msg = {"blocks": [{"text": {"text": "hello world", "type": "mrkdwn"}, "type": "section"}, {"type": "divider"}], "channel": "ec2", "icon_emoji": ":syringe:", "username": "bob"}
print(test.send_message(msg))