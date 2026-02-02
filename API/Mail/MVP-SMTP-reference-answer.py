import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr, make_msgid
from datetime import datetime
import re

class EmailReply:
    def __init__(self, smtp_server, smtp_port, username, password):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
    
    def create_reply(self, original_msg, reply_text, sender_email, sender_name=""):
        """
        Создает ответное письмо с сохранением истории переписки
        
        Args:
            original_msg: Оригинальное письмо (email.message.Message)
            reply_text: Текст ответа
            sender_email: Email отправителя ответа
            sender_name: Имя отправителя
        """
        # Создаем multipart сообщение
        msg = MIMEMultipart('alternative')
        
        # === 1. ЗАГОЛОВКИ ДЛЯ THREADING ===
        original_msg_id = original_msg.get('Message-ID', '').strip()
        original_references = original_msg.get('References', '').strip()
        
        # Subject с Re:
        original_subject = original_msg.get('Subject', 'No Subject')
        if not original_subject.lower().startswith('re:'):
            msg['Subject'] = f"Re: {original_subject}"
        else:
            msg['Subject'] = original_subject
        
        # From и To
        msg['From'] = formataddr((sender_name, sender_email))
        msg['To'] = original_msg.get('From', '')
        
        # In-Reply-To - указывает на непосредственное родительское сообщение
        if original_msg_id:
            msg['In-Reply-To'] = original_msg_id
        
        # References - цепочка всех сообщений в треде
        if original_references and original_msg_id:
            msg['References'] = f"{original_references} {original_msg_id}"
        elif original_msg_id:
            msg['References'] = original_msg_id
        
        # Генерируем уникальный Message-ID для нашего ответа
        msg['Message-ID'] = make_msgid(domain=sender_email.split('@')[1])
        msg['Date'] = datetime.now().strftime('%a, %d %b %Y %H:%M:%S %z')
        
        # === 2. ТЕЛО ПИСЬМА С ИСТОРИЕЙ ===
        
        # Получаем информацию из оригинального письма
        original_from = original_msg.get('From', 'Unknown')
        original_date = original_msg.get('Date', '')
        original_to = original_msg.get('To', '')
        
        # Извлекаем текст оригинального письма
        original_text = self._extract_text_from_message(original_msg)
        original_html = self._extract_html_from_message(original_msg)
        
        # === 2.1 PLAIN TEXT версия ===
        plain_text = self._create_plain_text_reply(
            reply_text, 
            original_text, 
            original_from, 
            original_date,
            original_to
        )
        
        # === 2.2 HTML версия ===
        html_text = self._create_html_reply(
            reply_text,
            original_html or original_text,
            original_from,
            original_date,
            original_to
        )
        
        # Добавляем обе версии
        part1 = MIMEText(plain_text, 'plain', 'utf-8')
        part2 = MIMEText(html_text, 'html', 'utf-8')
        
        msg.attach(part1)
        msg.attach(part2)
        
        return msg
    
    def _extract_text_from_message(self, msg):
        """Извлекает plain text из письма"""
        text = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == 'text/plain':
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        text = payload.decode(charset, errors='ignore')
                        break
                    except:
                        text = payload.decode('utf-8', errors='ignore')
        else:
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or 'utf-8'
            try:
                text = payload.decode(charset, errors='ignore')
            except:
                text = payload.decode('utf-8', errors='ignore')
        
        return text.strip()
    
    def _extract_html_from_message(self, msg):
        """Извлекает HTML из письма"""
        html = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == 'text/html':
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        html = payload.decode(charset, errors='ignore')
                        break
                    except:
                        html = payload.decode('utf-8', errors='ignore')
        else:
            if msg.get_content_type() == 'text/html':
                payload = msg.get_payload(decode=True)
                charset = msg.get_content_charset() or 'utf-8'
                try:
                    html = payload.decode(charset, errors='ignore')
                except:
                    html = payload.decode('utf-8', errors='ignore')
        
        return html.strip()
    
    def _create_plain_text_reply(self, reply_text, original_text, 
                                  original_from, original_date, original_to):
        """Создает plain text версию с цитированием"""
        
        # Добавляем > перед каждой строкой оригинального текста
        quoted_text = '\n'.join(['> ' + line for line in original_text.split('\n')])
        
        plain_reply = f"""{reply_text}

On {original_date}, {original_from} wrote:
{quoted_text}
"""
        return plain_reply
    
    def _create_html_reply(self, reply_text, original_content, 
                           original_from, original_date, original_to):
        """Создает HTML версию с цитированием"""
        
        # Экранируем reply_text для HTML
        reply_html = reply_text.replace('\n', '<br>\n')
        
        # Если оригинал был plain text, конвертируем в HTML
        if not original_content.strip().startswith('<'):
            original_html = original_content.replace('\n', '<br>\n')
        else:
            original_html = original_content
        
        html_reply = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
</head>
<body>
    <div>{reply_html}</div>
    <br>
    <div style="border-left: 2px solid #ccc; padding-left: 10px; margin-top: 10px; color: #666;">
        <div style="margin-bottom: 10px;">
            <strong>On {original_date}, {original_from} wrote:</strong>
        </div>
        <div>
            {original_html}
        </div>
    </div>
</body>
</html>
"""
        return html_reply
    
    def send_reply(self, msg, recipient_email):
        """Отправляет ответное письмо"""
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
                print(f"✓ Письмо успешно отправлено на {recipient_email}")
                return True
        except Exception as e:
            print(f"✗ Ошибка отправки: {e}")
            return False


# === ПРИМЕР ИСПОЛЬЗОВАНИЯ ===

def example_usage():
    import imaplib
    import email
    
    # Настройки
    IMAP_SERVER = "imap.yandex.ru"
    SMTP_SERVER = "smtp.yandex.ru"
    SMTP_PORT = 587
    EMAIL = "" #email сотрудника, под которым идёт отправка
    PASSWORD = "" # пароль приложений
    
    # 1. Получаем письмо по IMAP
    imap = imaplib.IMAP4_SSL(IMAP_SERVER)
    imap.login(EMAIL, PASSWORD)
    imap.select('INBOX')
    
    # Ищем последнее письмо
    _, message_numbers = imap.search(None, 'ALL')
    latest_email_id = message_numbers[0].split()[-1]
    
    # Скачиваем письмо
    _, msg_data = imap.fetch(latest_email_id, '(RFC822)')
    email_body = msg_data[0][1]
    original_msg = email.message_from_bytes(email_body)
    
    imap.close()
    imap.logout()
    
    # 2. Создаем и отправляем ответ
    reply_handler = EmailReply(SMTP_SERVER, SMTP_PORT, EMAIL, PASSWORD)
    
    reply_text = """Здравствуйте!

Спасибо за ваше письмо. Я получил вашу информацию и готов обсудить детали.

С уважением,
Иван"""
    
    # Создаем ответное письмо
    reply_msg = reply_handler.create_reply(
        original_msg=original_msg,
        reply_text=reply_text,
        sender_email=EMAIL,
        sender_name="Иван Иванов"
    )
    
    # Отправляем
    recipient = original_msg.get('From', '')
    reply_handler.send_reply(reply_msg, recipient)


if __name__ == "__main__":
    example_usage()
