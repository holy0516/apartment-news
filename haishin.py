# -*- coding: utf-8 -*-
# 新着物件を LINE broadcast で配信
# 必要な環境変数: LINE_CHANNEL_TOKEN（必須）
import os
import time
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd

LINE_BROADCAST_URL = "https://api.line.me/v2/bot/message/broadcast"
CHANNEL_TOKEN = os.getenv("LINE_CHANNEL_TOKEN")
MAX_LEN = int(os.getenv("LINE_MAX_LEN", "2000"))
JST = timezone(timedelta(hours=9))


def build_lines(df: pd.DataFrame):
    lines = []
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
    for _, row in df.iterrows():
        name = row.get("物件名", "")
        price = row.get("価格", "")
        url = row.get("URL", "")
        lines.append(f"・{name} / {price}\n{url}")
    return lines


def chunk_messages(lines, max_len):
    header = f"新着物件 {datetime.now(JST).strftime('%Y-%m-%d %H:%M')}\n"
    chunks, buf = [], header
    for line in lines:
        candidate = f"{buf}\n{line}" if buf else line
        if len(candidate) <= max_len:
            buf = candidate
        else:
            chunks.append(buf)
            buf = f"{header}{line}"
    if buf:
        chunks.append(buf)
    return chunks


def broadcast(text: str) -> None:
    if not CHANNEL_TOKEN:
        raise RuntimeError("LINE_CHANNEL_TOKEN が未設定です。")
    headers = {
        "Authorization": f"Bearer {CHANNEL_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"messages": [{"type": "text", "text": text}]}

    for attempt in range(3):
        resp = requests.post(LINE_BROADCAST_URL, headers=headers, json=payload, timeout=20)
        if resp.status_code == 429 and attempt < 2:
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
        return


def main():
    # -------------------------
    # ✅ ここであなたのスクレイパーの結果を読む
    # -------------------------
    df = pd.read_csv("new_items.csv")   # 例: 新着物件の CSV
    if df.empty:
        print("配信対象なし（新着ゼロ）")
        return

    # 価格に「億」が含まれる物件だけ残す
    df = df[df["価格"].str.contains("億", na=False)]

    lines = build_lines(df)
    messages = chunk_messages(lines, MAX_LEN)

    for m in messages:
        broadcast(m)
        time.sleep(1)

    print(f"配信完了: {len(df)}件 / メッセージ {len(messages)} 通")


if __name__ == "__main__":
    main()
