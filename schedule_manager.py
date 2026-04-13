import json
import os
from datetime import datetime, timedelta
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

class ScheduleManager:
    def __init__(self, times_file='times.json', cache_file='cache/schedule.json',
                 cache_meta_file='cache/cache_meta.json'):
        self.times_file = times_file
        self.cache_file = cache_file
        self.cache_meta_file = cache_meta_file
        self._times_data = None
        self._cache_memory = None
        self._cache_mtime = 0
        os.makedirs(os.path.dirname(cache_file) or '.', exist_ok=True)

    @property
    def times_data(self):
        if self._times_data is None:
            try:
                with open(self.times_file, 'r', encoding='utf-8') as f:
                    self._times_data = json.load(f)
            except Exception as e:
                logger.error(f"Ошибка загрузки times.json: {e}")
                self._times_data = {}
        return self._times_data

    def _get_day_code(self, date_obj):
        return ('пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс')[date_obj.weekday()]

    def _load_cache_meta(self):
        try:
            with open(self.cache_meta_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_cache_meta(self, meta):
        try:
            tmp_file = self.cache_meta_file + '.tmp'
            with open(tmp_file, 'w', encoding='utf-8') as f:
                json.dump(meta, f, ensure_ascii=False, separators=(',', ':'))
            os.replace(tmp_file, self.cache_meta_file)
        except Exception as e:
            logger.error(f"Ошибка сохранения метаданных: {e}")

    def _load_cache(self):
        if self._cache_memory is not None:
            try:
                if os.path.getmtime(self.cache_file) == self._cache_mtime:
                    return self._cache_memory
            except Exception:
                pass

        if not os.path.exists(self.cache_file):
            return {}

        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self._cache_memory = data
                self._cache_mtime = os.path.getmtime(self.cache_file)
                return data
        except Exception:
            return {}

    def _save_cache(self, schedule_by_date):
        try:
            tmp_file = self.cache_file + '.tmp'
            with open(tmp_file, 'w', encoding='utf-8') as f:
                json.dump(schedule_by_date, f, ensure_ascii=False, separators=(',', ':'))
            os.replace(tmp_file, self.cache_file)

            self._cache_memory = schedule_by_date
            self._cache_mtime = os.path.getmtime(self.cache_file)

            total_lessons = sum(len(lessons) for lessons in schedule_by_date.values())
            self._save_cache_meta({
                'last_update': datetime.now().isoformat(),
                'total_lessons': total_lessons
            })
            logger.info(f"Кэш сохранён: {total_lessons} уроков")
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша: {e}")

    def is_cache_fresh(self, max_age_hours=24):
        meta = self._load_cache_meta()
        if not meta or 'last_update' not in meta:
            return False
        try:
            last_update = datetime.fromisoformat(meta['last_update'])
            return (datetime.now() - last_update) < timedelta(hours=max_age_hours)
        except Exception:
            return False

    @lru_cache(maxsize=128)
    def get_cache_age_text(self):
        meta = self._load_cache_meta()
        if not meta or 'last_update' not in meta:
            return "кэш отсутствует"
        try:
            last_update = datetime.fromisoformat(meta['last_update'])
            age = datetime.now() - last_update
            if age < timedelta(minutes=1):
                return "только что"
            elif age < timedelta(hours=1):
                return f"{int(age.seconds / 60)} мин. назад"
            elif age < timedelta(days=1):
                return f"{int(age.seconds / 3600)} ч. назад"
            else:
                return f"{age.days} дн. назад"
        except Exception:
            return "неизвестно"

    def _combine_lessons_with_time(self, lessons, day_code):
        day_times = self.times_data.get(day_code, {})
        result = []
        for lesson in lessons:
            lesson_num = lesson['number']
            time_info = day_times.get(f"урок {lesson_num}", {"timebegin": "--:--", "timeend": "--:--"})
            result.append({
                'number': lesson_num,
                'name': lesson['name'],
                'timebegin': time_info['timebegin'],
                'timeend': time_info['timeend']
            })
        return result

    def update_schedule_from_email(self, email_parsed_data):
        cache = self._load_cache()
        for date_obj, day_schedules in email_parsed_data.items():
            date_str = date_obj.isoformat() if hasattr(date_obj, 'isoformat') else str(date_obj)
            for day_code, lessons in day_schedules.items():
                cache[date_str] = self._combine_lessons_with_time(lessons, day_code)
                logger.info(f"Обновлено {date_str} ({day_code}): {len(lessons)} уроков")
        self._save_cache(cache)
        return cache

    def get_schedule_by_date(self, target_date):
        date_str = target_date.isoformat() if hasattr(target_date, 'isoformat') else str(target_date)
        return self._load_cache().get(date_str, [])

    def get_schedule_for_week(self, days_order):
        cache = self._load_cache()
        return {
            day_code: cache.get(date_obj.isoformat(), [])
            for date_obj, day_code, _ in days_order
        }

    def clear_cache(self):
        self._cache_memory = None
        self._cache_mtime = 0
        self.get_cache_age_text.cache_clear()

        # Удаляем только файлы расписания
        for f in [self.cache_file, self.cache_meta_file]:
            try:
                os.remove(f)
                logger.info(f"Удалён файл: {f}")
            except FileNotFoundError:
                pass
            except Exception as e:
                logger.error(f"Ошибка удаления {f}: {e}")

        return True

    def get_cache_info(self):
        meta = self._load_cache_meta()
        cache = self._load_cache()
        days_count = {}
        for date_str in cache.keys():
            try:
                date_obj = datetime.fromisoformat(date_str).date()
                day_code = self._get_day_code(date_obj)
                days_count[day_code] = days_count.get(day_code, 0) + 1
            except Exception:
                pass

        return {
            'last_update': meta.get('last_update', 'неизвестно'),
            'age_text': self.get_cache_age_text(),
            'dates': list(cache.keys()),
            'days': list(days_count.keys()),
            'total_lessons': sum(len(l) for l in cache.values()),
            'is_fresh': self.is_cache_fresh()
        }