#!/usr/bin/env python3

import slack_sdk as slack
import os
from flask import Flask
from pathlib import Path
from slackeventsapi import SlackEventAdapter
from dotenv import load_dotenv
from influxdb import InfluxDBClient
from datetime import datetime

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(os.environ['SIGNING_SECRET'], '/counting-game/slack/events', app)



def log_to_influx(channel_id, ts, number):
    ifclnt = InfluxDBClient(
            os.environ['IF_HOST'],
            os.environ['IF_PORT'],
            database=os.environ['IF_DB'],
            username=os.environ['IF_USERNAME'],
            password=os.environ['IF_PASSWORD'])

    json_body = [
        {
            "measurement": "countinggame",
            "tags": {
                "channel_id": channel_id
            },
            "fields": {
                "number": number
            },
            "time": ts
        }
    ]
    print(json_body)
    ifclnt.write_points(json_body)
 
def process_message(channel, m):
    text = m.get('text')
    ts = m.get('ts')
    ts_s = datetime.utcfromtimestamp(int(float(ts))).strftime('%Y-%m-%d %H:%M:%S')
    try:
        number = int(text)
        log_to_influx(channel, ts_s, number)
    except ValueError:
        pass


@slack_event_adapter.on('message')
def message(payload):
    event = payload.get('event', {})
    subtype = event.get('subtype')
    if subtype is not None:
        return

    channel_id = event.get('channel')
    process_message(channel_id, event)


def history(channel):
    client = slack.WebClient(token=os.environ['SLACK_TOKEN'])
    response = client.conversations_history(channel=channel, limit=110)
    messages = response.get('messages')
    for m in messages:
        history_message(channel, m)

    while response['has_more']:
        response = client.conversations_history(channel=channel, limit=3, cursor=response['response_metadata']['next_cursor'])
        messages = response.get('messages')
        for m in messages:
            process_message(channel, m)

    exit()

if __name__ == "__main__":
    app.run(debug=True, port=5000)
