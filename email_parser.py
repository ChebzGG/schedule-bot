import imaplib
import email
from email.header import decode_header
from datetime import datetime, timedelta
import re
from bs4 import BeautifulSoup
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

logger = logging.getLogger(__name__)


class EmailParser:
    def __init__(self, email_user, email_password, imap_server='imap.gmail.com'):
        self.email_user = email_user
        self.email_password = email_password
        self.imap_server = imap_server
        self._init_patterns()
        self._compiled_patterns = {}

    def _init_patterns(self):
        self.date_day_pattern = re.compile(
            r'на[\s\xa0]+(?:завтра|сегодня)[\s\xa0]*:?\s*(\d{1,2})[\s\xa0]+([а-я]+)[\s\xa0]+(\d{4})[\s\xa0]*г\.?[\s\xa0]*[-–]?[\s\xa0]*([А-Яа-я]+)[\s\xa0]*:',
            re.IGNORECASE
        )

        self.lesson_pattern = re.compile(
            r'^[\s\xa0]*(\d+)[\s\xa0]+пара[\s\xa0]+(\d+)[\s\xa0]+час[\s\xa0]*(пдгр\.\d+)?[\s\xa0]*(.+?)(?:[\s\xa0]*[-–]\s*(\d+(?:-\d+)?))?\s*$',
            re.IGNORECASE
        )

        self.lesson_pattern2 = re.compile(
            r'^[\s\xa0]*(\d+)[\s\xa0]+пара[\s\xa0]+(\d+)[\s\xa0]+час[\s\xa0]+(.+?)[\s\xa0]*$',
            re.IGNORECASE
        )

        self.skip_pattern = re.compile(r'уважаемая|уважаемый|автоматическая рассылка|-{10,}', re.IGNORECASE)

        self.months = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
            'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
        }

        self.days_mapping = {
            'понедельник': 'пн', 'вторник': 'вт', 'среда': 'ср',
            'четверг': 'чт', 'пятница': 'пт', 'суббота': 'сб', 'воскресенье': 'вс'
        }

    def _get_cached_pattern(self, pattern_name, pattern_regex):
        if pattern_name not in self._compiled_patterns:
            self._compiled_patterns[pattern_name] = re.compile(pattern_regex, re.IGNORECASE)
        return self._compiled_patterns[pattern_name]

    def connect(self):
        try:
            mail = imaplib.IMAP4_SSL(self.imap_server, timeout=15)
            mail.login(self.email_user, self.email_password)
            return mail
        except Exception as e:
            logger.error(f"Ошибка подключения: {e}")
            return None

    def get_email_hash(self, msg) -> str:
        """Генерирует уникальный хеш письма для отслеживания дубликатов"""
        try:
            message_id = msg.get('Message-ID', '')
            date = msg.get('Date', '')
            subject = self._get_subject(msg)
            hash_content = f"{message_id}|{date}|{subject}"
            return hashlib.md5(hash_content.encode('utf-8')).hexdigest()[:16]
        except Exception:
            import random
            return hashlib.md5(f"{datetime.now()}{random.random()}".encode()).hexdigest()[:16]

    def search_emails(self, days_back=3, max_emails=50):
        """Поиск писем за указанное количество дней"""
        mail = self.connect()
        if not mail:
            return []

        try:
            mail.select('inbox', readonly=True)

            date_since = datetime.now() - timedelta(days=days_back)
            date_str = date_since.strftime('%d-%b-%Y')

            date_until = datetime.now() + timedelta(days=1)
            date_until_str = date_until.strftime('%d-%b-%Y')
            search_criteria = f'(SINCE "{date_str}" BEFORE "{date_until_str}")'

            typ, message_numbers = mail.search(None, search_criteria)
            if typ != 'OK':
                return []

            email_ids = message_numbers[0].split()

            logger.info(
                f"Найдено писем за {days_back} дней (с {date_since.strftime('%d.%m.%Y')} по {date_until.strftime('%d.%m.%Y')}): {len(email_ids)}")

            email_ids = email_ids[-max_emails:] if len(email_ids) > max_emails else email_ids
            emails = self._fetch_emails_parallel(mail, email_ids)

            mail.close()
            mail.logout()
            return emails
        except Exception as e:
            logger.error(f"Ошибка поиска: {e}")
            try:
                mail.logout()
            except:
                pass
            return []

    def search_emails_with_hash(self, days_back=3, max_emails=50):
        """Поиск писем с их хешами"""
        emails = self.search_emails(days_back, max_emails)
        result = []
        for msg in emails:
            email_hash = self.get_email_hash(msg)
            result.append({
                'message': msg,
                'hash': email_hash,
                'subject': self._get_subject(msg),
                'date': msg.get('Date', '')
            })
        return result

    def _fetch_emails_parallel(self, mail, email_ids, max_workers=5):
        emails = []
        chunk_size = 10
        for i in range(0, len(email_ids), chunk_size):
            chunk = email_ids[i:i + chunk_size]
            chunk_ids = b','.join(chunk)
            try:
                typ, msg_data = mail.fetch(chunk_ids, '(RFC822)')
                if typ == 'OK':
                    for data in msg_data:
                        if isinstance(data, tuple):
                            emails.append(email.message_from_bytes(data[1]))
            except Exception as e:
                logger.warning(f"Ошибка batch-загрузки: {e}")
                for eid in chunk:
                    try:
                        typ, msg_data = mail.fetch(eid, '(RFC822)')
                        if typ == 'OK' and msg_data[0]:
                            emails.append(email.message_from_bytes(msg_data[0][1]))
                    except Exception as e2:
                        logger.warning(f"Ошибка загрузки {eid.decode()}: {e2}")

        return emails

    def _get_subject(self, msg):
        subject = msg.get("Subject", "")
        if not subject:
            return ""
        try:
            decoded = decode_header(subject)
            return " ".join(
                part.decode(charset or 'utf-8', errors='ignore') if isinstance(part, bytes) else part
                for part, charset in decoded
            )
        except Exception:
            return str(subject)

    def extract_body(self, msg):
        try:
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type == "text/plain":
                        payload = part.get_payload(decode=True)
                        if payload:
                            charset = part.get_content_charset() or 'utf-8'
                            text = payload.decode(charset, errors='ignore')
                            return re.sub(r'[\xa0\u2009\u202f]', ' ', text)
                    elif content_type == "text/html":
                        payload = part.get_payload(decode=True)
                        if payload:
                            charset = part.get_content_charset() or 'utf-8'
                            html = payload.decode(charset, errors='ignore')
                            text = re.sub(r'<[^>]+>', ' ', html)
                            text = re.sub(r'\s+', ' ', text)
                            return re.sub(r'[\xa0\u2009\u202f]', ' ', text)
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    charset = msg.get_content_charset() or 'utf-8'
                    text = payload.decode(charset, errors='ignore')
                    return re.sub(r'[\xa0\u2009\u202f]', ' ', text)
        except Exception as e:
            logger.error(f"Ошибка извлечения тела: {e}")
        return ""

    def parse_schedule_from_text(self, text):
        result = {}
        lines = text.split('\n')
        current_date = None
        current_day_code = None
        lessons = []

        filtered_lines = []
        for line in lines:
            line = line.strip()
            if not line or len(line) < 3:
                continue
            if self.skip_pattern.search(line.lower()):
                continue
            filtered_lines.append(line)

        for line in filtered_lines:
            if 'на ' in line.lower() and ('сегодня' in line.lower() or 'завтра' in line.lower()):
                date_match = self.date_day_pattern.search(line)
                if date_match:
                    if current_date and lessons:
                        result.setdefault(current_date, {})
                        result[current_date].setdefault(current_day_code, []).extend(lessons)

                    day = int(date_match.group(1))
                    month_name = date_match.group(2).lower()
                    year = int(date_match.group(3))
                    day_name = date_match.group(4).lower()

                    month = self.months.get(month_name, 1)
                    current_date = datetime(year, month, day).date()
                    current_day_code = self.days_mapping.get(day_name)
                    lessons = []
                    continue

            if 'пара' in line:
                lesson_match = self.lesson_pattern.match(line) or self.lesson_pattern2.match(line)
                if lesson_match and current_date and current_day_code:
                    try:
                        para_num = int(lesson_match.group(1))
                        hour_num = int(lesson_match.group(2))

                        groups = lesson_match.groups()

                        if len(groups) >= 5:
                            pdgr_prefix = lesson_match.group(3)
                            lesson_name = lesson_match.group(4).strip() if lesson_match.group(4) else ""
                            classroom = lesson_match.group(5)
                        else:
                            pdgr_prefix = None
                            lesson_name = lesson_match.group(3).strip() if lesson_match.group(3) else ""
                            classroom = None

                        lesson_type = "лаба" if pdgr_prefix else "лекция"

                        if classroom:
                            full_name = f"{lesson_name} {lesson_type} -{classroom}"
                        else:
                            full_name = f"{lesson_name} {lesson_type}"

                        lesson_number = (para_num - 1) * 2 + hour_num
                        lessons.append({'number': lesson_number, 'name': full_name})
                    except Exception as e:
                        logger.warning(f"Ошибка парсинга урока: {e}")

        if current_date and lessons:
            result.setdefault(current_date, {})
            result[current_date].setdefault(current_day_code, []).extend(lessons)

        for date_key in result:
            for day in result[date_key]:
                result[date_key][day].sort(key=lambda x: x['number'])

        if result:
            logger.info(f"✅ Распарсено: {len(result)} дат")
        return result

    def get_all_schedules(self, days_back=3, max_emails=50):
        emails = self.search_emails(days_back, max_emails)
        if not emails:
            logger.warning("Письма не найдены")
            return {}

        all_schedules = {}
        processed_count = 0
        skipped_count = 0

        keywords = {'распис', 'пара', 'урок', 'аудитория', 'к.'}

        for idx, msg in enumerate(emails):
            try:
                subject = self._get_subject(msg)
                subject_lower = subject.lower()

                if not any(kw in subject_lower for kw in keywords):
                    body_preview = self.extract_body(msg)[:500].lower()
                    if not any(kw in body_preview for kw in keywords):
                        skipped_count += 1
                        continue

                body = self.extract_body(msg)
                if not body:
                    skipped_count += 1
                    continue

                parsed = self.parse_schedule_from_text(body)
                if parsed:
                    processed_count += 1
                    for date_obj, day_schedules in parsed.items():
                        all_schedules.setdefault(date_obj, {})
                        for day, lessons in day_schedules.items():
                            if day not in all_schedules[date_obj]:
                                all_schedules[date_obj][day] = []

                            existing_numbers = {l['number'] for l in all_schedules[date_obj][day]}
                            for lesson in lessons:
                                if lesson['number'] not in existing_numbers:
                                    all_schedules[date_obj][day].append(lesson)
                                    existing_numbers.add(lesson['number'])
                            all_schedules[date_obj][day].sort(key=lambda x: x['number'])
                else:
                    skipped_count += 1

            except Exception as e:
                logger.warning(f"Ошибка обработки письма #{idx + 1}: {e}")
                continue

        logger.info(f"📊 Обработано: {processed_count} писем с расписанием, пропущено: {skipped_count}")
        total = sum(len(l) for ds in all_schedules.values() for l in ds.values())
        logger.info(f"ИТОГО: {len(all_schedules)} дат, {total} уроков")
        return all_schedules