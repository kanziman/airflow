import requests
from airflow.models import Variable


def find_chat_id():
    __TOKEN = Variable.get('telegram_token')
    url = f"https://api.telegram.org/bot{__TOKEN}/getUpdates?"
    res = requests.get(url).json()
    for item in res.get("result", []):
        if "channel_post" in item:
            chat = item["channel_post"]["sender_chat"]
            print(f"channel: {chat['title']}({chat['id']})")
            
def send_message(telegram_token, message, chat_id):
    data = {"chat_id" : chat_id, "text": message }
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage?"
    res = requests.post(url, json=data)
    print(res.json())
