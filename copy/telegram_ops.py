# telegram_pusher.py
import time, threading, queue, requests, logging

class TelegramPusher:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *a, **kw):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self, token: str, chat_map: dict[str, int], max_per_sec=28):
        if hasattr(self, "_ready"):          # 只初始化一次
            return
        self._ready = True
        self.token      = token
        self.chat_map   = chat_map
        self.max_rate   = max_per_sec       # 留 2 条余量
        self.q          = queue.Queue()
        self.worker_th  = threading.Thread(target=self._worker, daemon=True)
        self.worker_th.start()

    # ---------- 对外 API ----------
    def send_message(self, user: str, text: str):
        chat_id = self.chat_map.get(user)
        if chat_id is None:
            logging.warning("No chat_id configured for %s – skip Telegram push", user)
            return
        self.q.put((chat_id, text))

    # ---------- 内部线程 ----------
    def _worker(self):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        sess = requests.Session()
        sent, t0 = 0, time.time()
        per_chat_last = {}   # chat_id -> last_ts  单聊 1 msg/s

        while True:
            try:
                chat_id, text = self.q.get(timeout=0.2)
            except queue.Empty:
                continue

            # 简易速率限制
            now = time.time()
            if now - t0 >= 1:
                sent, t0 = 0, now
            # 全局 30 msg/s
            if sent >= self.max_rate:
                time.sleep(max(0.05, 1 - (now - t0)))
                continue
            # 单聊 1 msg/s
            delta = now - per_chat_last.get(chat_id, 0)
            if delta < 1:
                time.sleep(1 - delta)
                continue

            payload = {"chat_id": chat_id,
                       "text": text[:4096],       # 4096 cap
                       "parse_mode": "MarkdownV2"}
            try:
                r = sess.post(url, json=payload, timeout=10).json()
                if not r.get("ok"):
                    # 429 - retry_after
                    if r.get("error_code") == 429:
                        sleep = r.get("parameters", {}).get("retry_after", 1)
                        # 指数退避：最多 60 s
                        sleep = min(sleep * 2, 60)
                        time.sleep(sleep)
                        self.q.put((chat_id, text))
                    else:
                        logging.error("TG push fail: %s", r)
                else:
                    sent += 1               # 全局滑窗自增
                    per_chat_last[chat_id] = now
            except Exception as e:
                logging.error("TG push exc: %s", e)

