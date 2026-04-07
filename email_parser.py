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
        # Компилируем regex один раз для скорости
        self._compiled_patterns = {}

    def _init_patterns(self):
        # ✅ Гибкий паттерн: допускает \xa0, отсутствие пробела после : и разные тире
        self.date_day_pattern = re.compile(
            r'на[\s\xa0]+(?:завтра|сегодня)[\s\xa0]*:[\s\xa0]*(\d{1,2})[\s\xa0]+([а-я]+)[\s\xa0]+(\d{4})[\s\xa0]*г\.?[\s\xa0]*[-–][\s\xa0]*([А-Яа-я]+)[\s\xa0]*:',
            re.IGNORECASE
        )

        # Паттерн для урока: устойчив к лишним пробелам и формату "пдгр.1"
        self.lesson_pattern = re.compile(
            r'^[\s\xa0]*(\d+)[\s\xa0]+пара[\s\xa0]+(\d+)[\s\xa0]+час[\s\xa0]*(пдгр\.\d+[\s\xa0]+)?(.+?)(?:[\s\xa0]*[-–][\s\xa0]*(\d+))?[\s\xa0]*$',
            re.IGNORECASE
        )

        self.lesson_pattern2 = re.compile(
            r'^[\s\xa0]*(\d+)[\s\xa0]+пара[\s\xa0]+(\d+)[\s\xa0]+час[\s\xa0]+(.+?)[\s\xa0]*$',
            re.IGNORECASE
        )

        self.skip_pattern = re.compile(r'уважаемая|уважаемый|автоматическая рассылка|-{10,}', re.IGNORECASE)

        # ✅ ИСПРАВЛЕНО: октябрь = 10
        self.months = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
            'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
        }

        self.days_mapping = {
            'понедельник': 'пн', 'вторник': 'вт', 'среда': 'ср',
            'четверг': 'чт', 'пятница': 'пт', 'суббота': 'сб', 'воскресенье': 'вс'
        }

    def _get_cached_pattern(self, pattern_name, pattern_regex):
        """Кэширует скомпилированные regex для ускорения"""
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

    def search_emails(self, days_back=3, max_emails=50):
        """Поиск писем за указанное количество дней (по умолчанию 3 дня)"""
        mail = self.connect()
        if not mail:
            return []

        try:
            mail.select('inbox', readonly=True)

            # ✅ Ищем письма за days_back дней (вчера, сегодня, завтра)
            # Завтрашние письма могут быть уже отправлены, поэтому берем с запасом
            date_since = datetime.now() - timedelta(days=days_back)
            date_str = date_since.strftime('%d-%b-%Y')
            search_criteria = f'SINCE "{date_str}"'

            # Также ищем письма, которые могут прийти завтра (если они уже есть)
            date_until = datetime.now() + timedelta(days=1)
            date_until_str = date_until.strftime('%d-%b-%Y')
            search_criteria = f'(SINCE "{date_str}" BEFORE "{date_until_str}")'

            typ, message_numbers = mail.search(None, search_criteria)
            if typ != 'OK':
                return []

            email_ids = message_numbers[0].split()

            # ✅ Логируем количество найденных писем с указанием периода
            logger.info(
                f"Найдено писем за {days_back} дней (с {date_since.strftime('%d.%m.%Y')} по {date_until.strftime('%d.%m.%Y')}): {len(email_ids)}")

            # Берем только последние N писем для ускорения
            email_ids = email_ids[-max_emails:] if len(email_ids) > max_emails else email_ids

            # Параллельная загрузка писем
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

    def _fetch_emails_parallel(self, mail, email_ids, max_workers=5):
        """Параллельная загрузка писем"""
        emails = []

        # Разбиваем на чанки для batch-загрузки
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
                # Fallback: загружаем по одному
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
        """Быстрое извлечение тела письма"""
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
                            # Быстрый парсинг HTML без создания полного DOM-дерева
                            # Удаляем теги простым regex для скорости
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
        """Оптимизированный парсинг текста"""
        result = {}
        lines = text.split('\n')
        current_date = None
        current_day_code = None
        lessons = []

        # Предварительная фильтрация строк
        filtered_lines = []
        for line in lines:
            line = line.strip()
            if not line or len(line) < 3:
                continue
            if self.skip_pattern.search(line.lower()):
                continue
            filtered_lines.append(line)

        for line in filtered_lines:
            # Быстрая проверка на дату
            if 'на ' in line.lower() and ('сегодня' in line.lower() or 'завтра' in line.lower()):
                date_match = self.date_day_pattern.search(line)
                if date_match:
                    # Сохраняем предыдущий день
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

            # Быстрая проверка на урок
            if 'пара' in line:
                lesson_match = self.lesson_pattern.match(line) or self.lesson_pattern2.match(line)
                if lesson_match and current_date and current_day_code:
                    try:
                        para_num = int(lesson_match.group(1))
                        hour_num = int(lesson_match.group(2))
                        
                        groups = lesson_match.groups()
                        
                        # Определяем, какой паттерн сработал (lesson_pattern имеет 5 групп, lesson_pattern2 - 3)
                        if len(groups) >= 5 and groups[2] is not None:
                            # Сработал lesson_pattern с префиксом "пдгр."
                            subgroup_prefix = groups[2]
                            is_lab = 'пдгр' in subgroup_prefix.lower()
                            lesson_name = groups[3].strip() if groups[3] else ''
                            classroom = groups[4] if len(groups) > 4 and groups[4] else None
                        elif len(groups) >= 5 and groups[2] is None:
                            # Сработал lesson_pattern без префикса "пдгр."
                            is_lab = False
                            lesson_name = groups[3].strip() if groups[3] else ''
                            classroom = groups[4] if len(groups) > 4 and groups[4] else None
                        else:
                            # Сработал lesson_pattern2 (только 3 группы)
                            is_lab = False
                            lesson_name = groups[2].strip() if len(groups) > 2 and groups[2] else ''
                            classroom = None
                        
                        # Если кабинет не найден в конце строки, пробуем найти его в названии
                        if not classroom:
                            classroom_match = re.search(r'[-–](\d+)$', lesson_name)
                            if classroom_match:
                                classroom = classroom_match.group(1)
                                lesson_name = lesson_name[:classroom_match.start()].strip()
                        
                        # Определяем тип занятия
                        lesson_type = "лаба" if is_lab else "лекция"
                        
                        # Формируем полное название с типом занятия и кабинетом
                        if classroom:
                            full_name = f"{lesson_name} ({lesson_type}, к.{classroom})"
                        else:
                            full_name = f"{lesson_name} ({lesson_type})"

                        lesson_number = (para_num - 1) * 2 + hour_num
                        lessons.append({'number': lesson_number, 'name': full_name})
                    except Exception as e:
                        logger.warning(f"Ошибка парсинга урока: {e}")

        # Сохраняем последний день
        if current_date and lessons:
            result.setdefault(current_date, {})
            result[current_date].setdefault(current_day_code, []).extend(lessons)

        # Быстрая сортировка
        for date_key in result:
            for day in result[date_key]:
                result[date_key][day].sort(key=lambda x: x['number'])

        if result:
            logger.info(f"✅ Распарсено: {len(result)} дат")
        return result

    def get_all_schedules(self, days_back=3, max_emails=50):
        """
        Основной метод получения расписания с оптимизациями

        Args:
            days_back: количество дней для поиска (по умолчанию 3 - вчера, сегодня, завтра)
            max_emails: максимальное количество писем для обработки
        """
        emails = self.search_emails(days_back, max_emails)
        if not emails:
            logger.warning("Письма не найдены")
            return {}

        all_schedules = {}
        processed_count = 0
        skipped_count = 0

        # Ключевые слова для быстрого фильтра (в виде set для O(1) поиска)
        keywords = {'распис', 'пара', 'урок', 'аудитория', 'к.'}

        for idx, msg in enumerate(emails):
            try:
                # Быстрая проверка темы
                subject = self._get_subject(msg)
                subject_lower = subject.lower()

                # Пропускаем письма, которые точно не про расписание
                if not any(kw in subject_lower for kw in keywords):
                    # Проверяем только начало тела для экономии времени
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

                            # Быстрое объединение с проверкой на дубликаты
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