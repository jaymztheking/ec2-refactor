import json

template = {
    "blocks": [
      {
      "text": {
        "text": "Your ingestion results are ready!!",
        "type": "mrkdwn"
      },
      "type": "section"
      },
      {
        "type": "divider"
      }
    ],
    "channel": "ec2-dev-incr-pipeline-monitor",
    "icon_emoji": ":syringe:",
    "username": "ec2-pipeline"
  }


#mydict = json.load(template)
mydict = template
mydict["channel"] = "ec2"
mydict["blocks"][0]["text"]["text"] = 'hello world'

print(json.dumps(mydict))
