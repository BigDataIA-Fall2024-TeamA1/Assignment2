# openai_module.py
import openai
import os
from dotenv import load_dotenv

# 加載 .env 文件中的環境變數
load_dotenv()

def send_to_openai(prompt):
    openai.api_key = os.getenv("OPENAI_API_KEY")
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",  # 修正模型名稱
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful AI assistant. Please answer the questions based on the content of the provided files, and respond in English."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            max_tokens=1500,  # 根據需要調整
            temperature=0.7,
        )
        advice = response.choices[0].message.content  # 正確訪問訊息內容
        return advice
    except Exception as e:
        return f"與 OpenAI 通信時發生錯誤: {e}"