# process_logs.py
import csv
import json
import os
import re
from collections import defaultdict

def process_logs(webhook_pixel_log_file, webhook_click_log_file, gha_sent_log_files_pattern, output_dir="results"):
    """
    Обрабатывает логи Webhook.site, Cloudflare Worker и GHA sent_log.csv, 
    создает отчеты по открытым и кликнутым письмам.
    gha_sent_log_files_pattern: Шаблон для поиска файлов sent_log.csv (например, 'path/to/extracted_logs/*/sent_log.csv')
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 1. Загрузка отправленных писем из логов GHA
    sent_emails_map = {} # email_uid -> {'recipient_email', 'sender_account', 'status', 'timestamp', 'runner_id'}
    
    # Собираем все sent_log.csv из всех ранеров
    import glob
    all_sent_logs = glob.glob(gha_sent_log_files_pattern, recursive=True)
    if not all_sent_logs:
        print(f"Ошибка: Не найдено файлов sent_log.csv по шаблону: {gha_sent_log_files_pattern}")
        return

    for log_file in all_sent_logs:
        try:
            with open(log_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['email_uid'] and row['recipient_email']:
                        sent_emails_map[row['email_uid']] = {
                            'recipient_email': row['recipient_email'],
                            'sender_account': row['sender_account'],
                            'status': row['status'],
                            'timestamp': row['timestamp'],
                            'runner_id': row.get('runner_id', 'N/A')
                        }
        except FileNotFoundError:
            print(f"Предупреждение: Файл логов GHA не найден: {log_file}")
    print(f"Загружено {len(sent_emails_map)} записей об отправленных письмах из GHA.")

    # 2. Обработка логов открытий (webhook.site)
    opened_emails = set()
    opened_emails_details = []
    try:
        with open(webhook_pixel_log_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    log_entry = json.loads(line.strip())
                    query_string = log_entry.get('request', {}).get('query_string', '')
                    uid_match = re.search(r'uid=([a-f0-9]+)', query_string)
                    if uid_match:
                        email_uid = uid_match.group(1)
                        if email_uid in sent_emails_map:
                            if email_uid not in opened_emails:
                                opened_emails.add(email_uid)
                                details = sent_emails_map[email_uid]
                                opened_emails_details.append({
                                    'recipient_email': details['recipient_email'],
                                    'sender_account': details['sender_account'],
                                    'email_uid': email_uid,
                                    'open_timestamp': log_entry.get('request', {}).get('timestamp', 'N/A')
                                })
                except json.JSONDecodeError:
                    print(f"Предупреждение: Не удалось разобрать строку JSON в логе пикселя: {line.strip()[:100]}...")
    except FileNotFoundError:
        print(f"Предупреждение: Файл логов пикселя Webhook.site не найден: {webhook_pixel_log_file}")
    
    print(f"Обнаружено {len(opened_emails)} уникальных открытий писем.")

    # 3. Обработка логов кликов (Cloudflare Worker)
    clicked_emails = set()
    clicked_emails_details = []
    try:
        with open(webhook_click_log_file, 'r', encoding='utf-8') as f:
            for line in f:
                uid_match = re.search(r'Click detected for UID: ([a-f0-9]+) from IP: ([\d\.]+) at (.*)', line)
                if uid_match:
                    email_uid = uid_match.group(1)
                    click_ip = uid_match.group(2)
                    click_timestamp = uid_match.group(3)
                    if email_uid in sent_emails_map:
                        if email_uid not in clicked_emails:
                            clicked_emails.add(email_uid)
                            details = sent_emails_map[email_uid]
                            clicked_emails_details.append({
                                'recipient_email': details['recipient_email'],
                                'sender_account': details['sender_account'],
                                'email_uid': email_uid,
                                'click_timestamp': click_timestamp,
                                'click_ip': click_ip
                            })
    except FileNotFoundError:
        print(f"Предупреждение: Файл логов кликов Cloudflare Worker не найден: {webhook_click_log_file}")

    print(f"Обнаружено {len(clicked_emails)} уникальных кликов по ссылкам.")

    # 4. Сохранение результатов
    # Отчет по открытым письмам
    with open(os.path.join(output_dir, 'opened_emails.csv'), 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['recipient_email', 'sender_account', 'email_uid', 'open_timestamp']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(opened_emails_details)
    print(f"Сформирован отчет: {os.path.join(output_dir, 'opened_emails.csv')}")

    # Отчет по кликнутым письмам
    with open(os.path.join(output_dir, 'clicked_emails.csv'), 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['recipient_email', 'sender_account', 'email_uid', 'click_timestamp', 'click_ip']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clicked_emails_details)
    print(f"Сформирован отчет: {os.path.join(output_dir, 'clicked_emails.csv')}")

    # Собираем список открывших email
    recipients_who_opened = [d['recipient_email'] for d in opened_emails_details]
    with open(os.path.join(output_dir, 'recipients_who_opened.txt'), 'w', encoding='utf-8') as f:
        for email in recipients_who_opened:
            f.write(f"{email}\n")
    print(f"Список открывших письма сохранен в: {os.path.join(output_dir, 'recipients_who_opened.txt')}")

    # Сводный отчет по всей рассылке
    total_successful_sends = len([e for e in sent_emails_map.values() if e['status'] == 'sent'])
    summary_data = {
        'total_attempted_sends_gha': len(sent_emails_map), # Сколько GHA-ранеры пытались отправить
        'total_sent_success': total_successful_sends,
        'total_opened': len(opened_emails),
        'total_clicked': len(clicked_emails),
        'open_rate': f"{len(opened_emails) / total_successful_sends:.2%}" if total_successful_sends > 0 else "0.00%",
        'click_rate': f"{len(clicked_emails) / total_successful_sends:.2%}" if total_successful_sends > 0 else "0.00%"
    }
    with open(os.path.join(output_dir, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=4)
    print(f"Сформирован сводный отчет: {os.path.join(output_dir, 'summary.json')}")

if __name__ == "__main__":
    # !!! ЗАМЕНИ ЭТИ ПУТИ НА РЕАЛЬНЫЕ ПУТИ К ТВОИМ СКАЧАННЫМ/СОБРАННЫМ ЛОГАМ !!!
    # Как получить эти файлы, будет описано в разделе "Сбор и обработка логов"
    
    # Файл JSON с логами пикселя Webhook.site
    WEBHOOK_PIXEL_LOGS = "webhook_pixel_logs.json" 
    # Текстовый файл с логами кликов Cloudflare Worker (скопированный вручную)
    WEBHOOK_CLICK_LOGS = "worker_click_logs.txt" 
    # Шаблон для поиска всех sent_log.csv из разных ранеров GitHub Actions.
    # Пример: если ты распаковал все логи в папку 'gha_logs/', и внутри неё есть подпапки типа 'logs-12345-1', 'logs-12345-2',
    # то шаблон будет 'gha_logs/*/sent_log.csv'.
    # ИЛИ если ты скопировал все sent_log.csv в одну папку 'all_sent_logs_folder/', то 'all_sent_logs_folder/sent_log_*.csv'
    GHA_SENT_LOGS_PATTERN = "path/to/extracted_gha_logs/*/sent_log.csv" 

    print("Запуск обработки логов...")
    process_logs(WEBHOOK_PIXEL_LOGS, WEBHOOK_CLICK_LOGS, GHA_SENT_LOGS_PATTERN)
    print("Обработка логов завершена.")
