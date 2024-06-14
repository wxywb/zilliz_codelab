import requests
import os

class MiniMaxBot():
    def __init__(self):
        self.group_id = os.getenv("MINIMAX_GROUP_ID")
        self.api_key = os.getenv("MINIMAX_API_KEY")
    
        url = f"https://api.minimax.chat/v1/text/chatcompletion_pro?GroupId={self.group_id}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def generate(self, text, **kwargs):
        payload = {
          "model": "abab6.5-chat",
          "tokens_to_generate": 2048,
          "temperature": 0.1,
          "top_p": 0.95,
          "stream": False,
          "reply_constraints": {
            "sender_type": "BOT",
            "sender_name": "MM智能助理"
          },
          "sample_messages": [],
          "plugins": [],
          "messages": [
            {
              "sender_type": "USER",
              "sender_name": "用户",
              "text": f"{text}"
            }
          ],
          "bot_setting": [
            {
              "bot_name": "MM智能助理",
              "content": "MM智能助理是一款由MiniMax自研的，没有调用其他产品的接口的大型语言模型。MiniMax是一家中国科技公司，一直致力于进行大模型相关的研究。"
            }
          ]
        }
        for key, value in kwargs.items():
            payload[key].update(value)

        response = requests.post(url, headers=headers, json=payload)
        return response

bot = MiniMaxBot()    
ans = bot.generate("你是谁")
print(ans.json()["reply"])

