# mailer_worker.py
import asyncio
import aiosmtplib
from email.message import EmailMessage
from email.headerregistry import Address
import random
import time
import uuid
import re
import os
import sys
import csv
from datetime import datetime

# Загружаем настройки из config.py
from config import *

# ============ Вспомогательные функции для рандомизации ============

def load_resource(filename):
    filepath = os.path.join("resources", filename)
    if not os.path.exists(filepath):
        print(f"Ошибка: Файл ресурсов не найден: {filepath}. Создайте и заполните его.")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

# Загрузка всех ресурсов в начале (чтобы не читать каждый раз)
NAMES = load_resource("names.txt")
SURNAMES = load_resource("surnames.txt")
SUBJECT_TEMPLATES = load_resource("subject_templates.txt")
BODY_START_TEMPLATES = load_resource("body_start.txt")
BODY_MAIN_TEMPLATES = load_resource("body_main.txt")
BODY_LINK_PHRASES = load_resource("body_link_phrases.txt")
BODY_END_TEMPLATES = load_resource("body_end.txt")
INVISIBLE_WORDS = load_resource("invisible_words.txt")
USER_AGENTS = load_resource("user_agents.txt")

# Пути к файлам логов (будут созданы на каждом GHA ранере)
SENT_LOG_FILE = "sent_log.csv"
ERROR_LOG_FILE = "error_log.csv"

def init_log_files():
    """Инициализирует CSV-файлы логов с заголовками, если их нет."""
    if not os.path.exists(SENT_LOG_FILE):
        with open(SENT_LOG_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'recipient_email', 'sender_account', 'email_uid', 'status', 'runner_id'])
    if not os.path.exists(ERROR_LOG_FILE):
        with open(ERROR_LOG_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'recipient_email', 'sender_account', 'error_type', 'error_message', 'runner_id'])

def log_event(log_file, data):
    """Записывает событие в указанный лог-файл."""
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(data)

def spintax(text):
    """Рекурсивно обрабатывает спинтакс-шаблоны."""
    while '{' in text and '}' in text:
        start = text.rfind('{')
        end = text.find('}', start)
        if start == -1 or end == -1: 
            break
        
        options = text[start+1:end].split('|')
        text = text[:start] + random.choice(options) + text[end+1:]
    return text

def get_random_sender_name():
    """Генерирует случайное имя отправителя в разных форматах."""
    if not NAMES or not SURNAMES: 
        return "Друг" 

    choice = random.randint(1, 4)
    if choice == 1: # Имя Фамилия
        return f"{random.choice(NAMES)} {random.choice(SURNAMES)}"
    elif choice == 2: # Только имя
        return random.choice(NAMES)
    elif choice == 3: # Имя Ф.
        return f"{random.choice(NAMES)} {random.choice(SURNAMES)[0]}."
    else: # Имя-Фамилия (как ник)
        return f"{random.choice(NAMES)}-{random.choice(SURNAMES)}"

def get_human_like_user_agent():
    """Возвращает случайный User-Agent для заголовка X-Mailer."""
    return random.choice(USER_AGENTS) if USER_AGENTS else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

def add_invisible_noise(html_content):
    """Добавляет невидимые символы, слова и HTML-комментарии."""
    noisy_content = ""
    for char in html_content:
        noisy_content += char
        if random.random() < 0.03: 
            noisy_content += random.choice(['\u200b', '\u200c', '\u200d', '\ufeff']) 
    
    if INVISIBLE_WORDS:
        for _ in range(random.randint(MIN_INVISIBLE_NOISE_BLOCKS, MAX_INVISIBLE_NOISE_BLOCKS)):
            noise_word_count = random.randint(3, 10)
            noise_text = " ".join(random.sample(INVISIBLE_WORDS, min(noise_word_count, len(INVISIBLE_WORDS))))
            noisy_content += f"<div style='display:none !important;font-size:0px;color:transparent;height:0;overflow:hidden;max-height:0;mso-hide:all;'>{noise_text}</div>"
    
    if random.random() < 0.7: 
        comment_text = f"<!-- {uuid.uuid4().hex[:10]} {random.choice(INVISIBLE_WORDS) if INVISIBLE_WORDS else ''} {int(time.time())} -->"
        parts = re.split(r'(</?\w+[^>]*>)', noisy_content)
        insert_idx = random.randint(0, len(parts))
        parts.insert(insert_idx, comment_text)
        noisy_content = "".join(parts)

    return noisy_content

def mutate_html_structure(html_content):
    """Мутирует HTML-структуру письма, меняя хеш-сумму, но не вид."""
    mutated_content = html_content
    
    for _ in range(random.randint(MIN_HTML_MUTATIONS, MAX_HTML_MUTATIONS)):
        mutation_type = random.choice(["color", "css", "data_attr"])

        if mutation_type == "color":
            mutated_content = mutated_content.replace("#000000", "#000001").replace("#FFFFFF", "#FFFFFE")
            mutated_content = mutated_content.replace("rgb(0,0,0)", "rgb(0,0,1)").replace("rgb(255,255,255)", "rgb(255,255,254)")
        
        elif mutation_type == "css":
            def add_random_css(match):
                tag_content = match.group(1)
                css_prop = random.choice([
                    "line-height", "letter-spacing", "word-spacing", "text-indent"
                ])
                css_value = round(random.uniform(0.01, 1.5), 2)
                if 'style=' in tag_content:
                    return f"{tag_content[:-1]}{css_prop}:{css_value}{'px' if 'indent' in css_prop else 'em'};'>"
                else:
                    return f"{tag_content} style='{css_prop}:{css_value}{'px' if 'indent' in css_prop else 'em'};'>"
            mutated_content = re.sub(r'(<[a-z]+[^>]*>)', add_random_css, mutated_content, random.randint(1,3))
        
        elif mutation_type == "data_attr":
            def add_random_data_attr(match):
                tag_content = match.group(1)
                return f"{tag_content} data-{random.choice(['id','ref','val','key'])}='{uuid.uuid4().hex[:8]}'>"
            mutated_content = re.sub(r'(<[a-z]+[^>]*>)', add_random_data_attr, mutated_content, random.randint(1,3))

    return mutated_content

# ============ Класс Отправителя ============

class GhostSender:
    def __init__(self, accounts, targets, runner_id):
        self.runner_id = runner_id
        # Каждый GHA-ранер выбирает себе подмножество аккаунтов
        if len(accounts) > ACCOUNTS_POOL_PER_RUNNER:
            self.accounts_pool = random.sample(accounts, ACCOUNTS_POOL_PER_RUNNER)
        else:
            self.accounts_pool = accounts
        print(f"GHA Runner {self.runner_id}: Выбрано {len(self.accounts_pool)} аккаунтов из общего пула.")

        self.targets = targets
        # Статистика аккаунтов для текущего ранера
        self.acc_data = {
            acc: {"last_used": 0, "emails_sent_this_round": 0, "total_sent": 0} 
            for acc in self.accounts_pool
        }
        self.lock = asyncio.Lock() # Для синхронизации доступа к acc_data в асинхронном режиме

    async def generate_email(self, sender_acc, recipient_email, email_uid):
        """Генерирует уникальное письмо с уникальным ID."""
        user, _ = sender_acc.split(':')
        sender_domain = user.split('@')[1]
        
        recipient_name = recipient_email.split('@')[0].replace('.', ' ').replace('-', ' ').title()
        
        sender_name = get_random_sender_name()
        
        subject_template = random.choice(SUBJECT_TEMPLATES) if SUBJECT_TEMPLATES else "Привет!"
        subject = spintax(subject_template).replace("{Имя получателя}", recipient_name)
        
        # Ссылка для отслеживания КЛИКОВ (через Cloudflare Worker)
        click_tracking_link = f"{CLICK_REDIRECT_WORKER_URL}?uid={email_uid}" 
        link_phrase = spintax(random.choice(BODY_LINK_PHRASES) if BODY_LINK_PHRASES else "глянь тут")
        
        body_parts = []
        body_parts.append(spintax(random.choice(BODY_START_TEMPLATES) if BODY_START_TEMPLATES else "Привет!"))
        
        main_body_text = spintax(random.choice(BODY_MAIN_TEMPLATES) if BODY_MAIN_TEMPLATES else "Как дела?")
        if random.random() < 0.5: 
            main_body_with_link = f"{link_phrase} <a href='{click_tracking_link}'>{click_tracking_link}</a>. {main_body_text}"
        else: 
            main_body_with_link = f"{main_body_text} {link_phrase} <a href='{click_tracking_link}'>{click_tracking_link}</a>."
        body_parts.append(main_body_with_link.replace("{случайная_тема}", random.choice(INVISIBLE_WORDS) if INVISIBLE_WORDS else "тема")) 
        
        body_parts.append(spintax(random.choice(BODY_END_TEMPLATES) if BODY_END_TEMPLATES else "Хорошего дня!"))
        
        body_html = "<p>" + "</p><p>".join(body_parts) + "</p>"

        body_html = add_invisible_noise(body_html)
        body_html = mutate_html_structure(body_html)
        
        # Добавляем невидимую 1x1 картинку для отслеживания ОТКРЫТИЙ (через Webhook.site)
        if WEBHOOK_PIXEL_URL:
            pixel_url = f"{WEBHOOK_PIXEL_URL}?uid={email_uid}"
            body_html += f"<img src='{pixel_url}' width='1' height='1' style='display:none !important;visibility:hidden;opacity:0;mso-hide:all;'>"

        msg = EmailMessage()
        msg["From"] = Address(display_name=sender_name, addr_spec=user)
        msg["To"] = recipient_email
        msg["Subject"] = subject
        msg["Reply-To"] = user 

        msg["Message-ID"] = f"<{email_uid}.{int(time.time())}@{sender_domain}>" 
        msg["Date"] = time.strftime("%a, %d %b %Y %H:%M:%S %z", time.gmtime())
        msg["X-Mailer"] = get_human_like_user_agent() 
        msg.set_content(body_html, subtype="html", charset="utf-8")
        
        return msg

    async def send_email_task(self, acc, recipient):
        """Задача по отправке одного письма с одного аккаунта."""
        user, password = acc.split(':')
        email_uid = uuid.uuid4().hex # Генерируем уникальный ID для этого письма
        
        # Проверяем лимиты и cooldown аккаунта в рамках текущего GHA-ранера
        async with self.lock:
            if self.acc_data[acc]["emails_sent_this_round"] >= random.randint(EMAILS_PER_ACC_MIN, EMAILS_PER_ACC_MAX):
                return False, "account_limit_reached" 
            if (time.time() - self.acc_data[acc]["last_used"]) < random.randint(COOLDOWN_ACC_MIN_SEC, COOLDOWN_ACC_MAX_SEC):
                return False, "account_cooldown" 
                
        msg = await self.generate_email(acc, recipient, email_uid)
        
        try:
            async with aiosmtplib.SMTP(
                hostname=SMTP_SERVER, port=SMTP_PORT, use_tls=USE_TLS,
                timeout=TIMEOUT_SEC
            ) as smtp:
                await smtp.login(user, password)
                await smtp.send_message(msg)
            
            async with self.lock: 
                self.acc_data[acc]["total_sent"] += 1
                self.acc_data[acc]["emails_sent_this_round"] += 1
                self.acc_data[acc]["last_used"] = time.time()
            
            print(f"✅ Отправлено ({self.runner_id}): {user} -> {recipient} (Всего с аккаунта: {self.acc_data[acc]['total_sent']}) UID: {email_uid}")
            log_event(SENT_LOG_FILE, [datetime.now().isoformat(), recipient, user, email_uid, 'sent', self.runner_id])
            
            # Имитация человеческой паузы между письмами с одного аккаунта
            await asyncio.sleep(random.uniform(*DELAY_BETWEEN_EMAILS_FROM_SAME_ACC_SEC))
            
            return True, None
        except aiosmtplib.SMTPAuthenticationError:
            error_msg = f"Ошибка авторизации на аккаунте {user}. Блокируем его для текущего раунда."
            print(f"❌ {error_msg}")
            log_event(ERROR_LOG_FILE, [datetime.now().isoformat(), recipient, user, 'SMTPAuthenticationError', error_msg, self.runner_id])
            async with self.lock:
                self.acc_data[acc]["emails_sent_this_round"] = 999999 
            return False, "auth_error"
        except aiosmtplib.SMTPRecipientsRefused as e:
            error_msg = f"Получатель {recipient} отказал в приеме письма от {user}: {e.recipients}"
            print(f"❌ {error_msg}")
            log_event(ERROR_LOG_FILE, [datetime.now().isoformat(), recipient, user, 'SMTPRecipientsRefused', error_msg, self.runner_id])
            log_event(SENT_LOG_FILE, [datetime.now().isoformat(), recipient, user, email_uid, 'refused', self.runner_id]) # Фиксируем отказ
            return False, "recipient_refused"
        except aiosmtplib.SMTPSenderRefused as e:
            error_msg = f"Отправитель {user} отказан сервером: {e.sender}. Блокируем его для текущего раунда."
            print(f"❌ {error_msg}")
            log_event(ERROR_LOG_FILE, [datetime.now().isoformat(), recipient, user, 'SMTPSenderRefused', error_msg, self.runner_id])
            async with self.lock:
                self.acc_data[acc]["emails_sent_this_round"] = 999999
            return False, "sender_refused"
        except Exception as e:
            error_msg = f"Критическая ошибка отправки с {user} на {recipient}: {type(e).__name__}: {e}"
            print(f"❌ {error_msg}")
            log_event(ERROR_LOG_FILE, [datetime.now().isoformat(), recipient, user, 'GenericError', error_msg, self.runner_id])
            return False, "generic_error"

    async def run_swarm_worker(self, targets_batch):
        """Запускает "рой" воркеров на одном GHA ранере."""
        print(f"GHA Runner {self.runner_id} запущен. Обрабатывает {len(targets_batch)} получателей.")
        recipients_queue = asyncio.Queue()
        for r in targets_batch:
            await recipients_queue.put(r)

        async def worker():
            """Отдельный асинхронный воркер, который берет получателей из очереди и отправляет."""
            while not recipients_queue.empty():
                recipient = await recipients_queue.get()
                
                chosen_acc = None
                # Перемешиваем аккаунты из пула текущего ранера
                shuffled_accounts = list(self.accounts_pool)
                random.shuffle(shuffled_accounts) 
                
                # Ищем доступный аккаунт, который "отдохнул" и не превысил лимит в этом раунде
                for acc in shuffled_accounts:
                    async with self.lock:
                        if self.acc_data[acc]["emails_sent_this_round"] < random.randint(EMAILS_PER_ACC_MIN, EMAILS_PER_ACC_MAX) \
                           and (time.time() - self.acc_data[acc]["last_used"]) >= random.randint(COOLDOWN_ACC_MIN_SEC, COOLDOWN_ACC_MAX_SEC):
                            chosen_acc = acc
                            break
                
                if chosen_acc:
                    success, reason = await self.send_email_task(chosen_acc, recipient)
                    if not success:
                        print(f"⚠️ GHA Runner {self.runner_id}: Не удалось отправить {recipient}. Причина: {reason}")
                else:
                    print(f"⚠️ GHA Runner {self.runner_id}: Нет доступных аккаунтов из пула для {recipient}. Пропускаем письмо.")
                    log_event(ERROR_LOG_FILE, [datetime.now().isoformat(), recipient, 'N/A', 'NoAvailableAccountInPool', 'No available sending account from current runner pool.', self.runner_id])
                
                recipients_queue.task_done()
                # Случайная микро-пауза между отправками для имитации несинхронности
                await asyncio.sleep(random.uniform(0.05, 0.2)) # Разница в милисекундах

        # Запускаем THREADS_PER_RUNNER количество воркеров для параллельной отправки
        await asyncio.gather(*[worker() for _ in range(THREADS_PER_RUNNER)])
        await recipients_queue.join() # Ждем завершения всех задач в очереди

async def main():
    runner_id = os.environ.get('GHA_RUNNER_ID', 'N/A')
    print(f"--- Запуск Swarm Emulation Engine (GHA Runner ID: {runner_id}) ---")
    print(f"Режим: {'ТЕСТОВЫЙ' if TEST_MODE else 'БОЕВОЙ'}")

    init_log_files() # Инициализируем файлы логов

    # Загрузка аккаунтов и целей из переменных окружения (GitHub Actions)
    # GHA будет передавать эти переменные
    accounts_str = os.environ.get("GHA_ACCOUNTS", "").strip()
    targets_str = os.environ.get("GHA_TARGETS", "").strip()

    all_accounts = [a for a in accounts_str.splitlines() if a.strip()]
    targets = [t for t in targets_str.splitlines() if t.strip()]

    if TEST_MODE:
        targets = TEST_EMAILS # В тестовом режиме используем только указанные тестовые адреса
        print(f"Тестовый режим: Отправка на {len(TEST_EMAILS)} тестовых адресов.")
    else:
        print(f"Боевой режим: Обработка {len(targets)} получателей.")

    if not all_accounts:
        print("Ошибка: Нет аккаунтов для GHA-ранера. Проверьте resources/accounts.txt и GHA workflow.")
        sys.exit(1)
    if not targets:
        print("Ошибка: Нет получателей. Заполните base.txt или TEST_EMAILS.")
        sys.exit(1)
    
    random.shuffle(all_accounts) # Перемешиваем аккаунты для лучшей ротации
    random.shuffle(targets)  # Перемешиваем получателей

    sender = GhostSender(all_accounts, targets, runner_id)
    
    # Сброс счетчиков "emails_sent_this_round" для всех аккаунтов в пуле ранера
    # и имитация "отдохнувших" аккаунтов
    async with sender.lock:
        for acc in sender.acc_data:
            sender.acc_data[acc]["emails_sent_this_round"] = 0
            if sender.acc_data[acc]["last_used"] == 0:
                # Имитация, что аккаунт давно не использовался и готов к работе
                sender.acc_data[acc]["last_used"] = time.time() - random.randint(COOLDOWN_ACC_MAX_SEC, COOLDOWN_ACC_MAX_SEC * 2)
                
    await sender.run_swarm_worker(targets)
            
    print(f"\n--- GHA Runner {runner_id} завершил работу! ---")

if __name__ == "__main__":
    # Проверка наличия папки resources и создание пустых файлов-заглушек
    if not os.path.exists("resources"):
        os.makedirs("resources")
        print("Создана папка 'resources'. Заполните её файлами!")
        resource_files = ["accounts.txt", "base.txt", "names.txt", "surnames.txt",
                          "subject_templates.txt", "body_start.txt", "body_main.txt",
                          "body_link_phrases.txt", "body_end.txt",
                          "invisible_words.txt", "user_agents.txt"]
        for res_file in resource_files:
            filepath = os.path.join("resources", res_file)
            if not os.path.exists(filepath):
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(f"# Заполните этот файл необходимыми данными с помощью ИИ (см. инструкции)\n# Файл: {res_file}\n")
                print(f"Создан пустой файл 'resources/{res_file}'.")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nРассылка прервана пользователем.")
