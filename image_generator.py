from PIL import Image, ImageDraw, ImageFont
import logging
import os
import re

logger = logging.getLogger(__name__)


class ScheduleImageGenerator:
    def __init__(self):
        self.width = 1400

        self.header_height = 140
        self.row_height = 90
        self.break_height = 56
        self.padding = 40

        self.colors = {
            'background': '#FFFFFF',
            'header_start': (123, 31, 162),
            'header_end': (74, 20, 140),
            'header_text': '#FFFFFF',
            'header_sub_text': '#E1BEE7',
            'row_even': '#F3E5F5',
            'row_odd': '#FFFFFF',
            'text': '#1A1A2E',
            'time_text': '#7B1FA2',
            'border': '#E1BEE7',
            'shadow_rgb': (40, 10, 60),
            'break_bg': '#FFD700',
            'break_bg_start': '#FFD54F',  # светлый край градиента
            'break_bg_end': '#FF8F00',  # тёмный край градиента
            'break_text': '#4E342E',
            'small_break': '#F5F5F5'
        }
        self.text_shadow_rgb = (20, 0, 40)

    def _hex_to_rgb(self, hex_color):
        """Преобразует HEX строку (#RRGGBB) в кортеж (R, G, B)"""
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))

    def _get_font(self, size=18, weight='regular'):
        font_files = {
            'extrabold': 'benzin-extrabold.ttf',
            'bold': 'benzin-bold.ttf',
            'semibold': 'benzin-semibold.ttf',
            'medium': 'benzin-medium.ttf',
            'regular': 'benzin-regular.ttf'
        }
        filename = font_files.get(weight, 'benzin-regular.ttf')
        font_path = os.path.join('Fonts', filename)
        try:
            return ImageFont.truetype(font_path, size)
        except Exception:
            return ImageFont.load_default()

    def _create_gradient_rect(self, draw, coords, start_rgb, end_rgb, horizontal=False):
        """Рисует прямоугольник с градиентом (вертикальным или горизонтальным)"""
        x1, y1, x2, y2 = coords
        if horizontal:
            steps = x2 - x1
            for i in range(steps):
                ratio = i / steps if steps != 0 else 0
                r = int(start_rgb[0] + (end_rgb[0] - start_rgb[0]) * ratio)
                g = int(start_rgb[1] + (end_rgb[1] - start_rgb[1]) * ratio)
                b = int(start_rgb[2] + (end_rgb[2] - start_rgb[2]) * ratio)
                draw.line([(x1 + i, y1), (x1 + i, y2)], fill=(r, g, b))
        else:
            steps = y2 - y1
            for i in range(steps):
                ratio = i / steps if steps != 0 else 0
                r = int(start_rgb[0] + (end_rgb[0] - start_rgb[0]) * ratio)
                g = int(start_rgb[1] + (end_rgb[1] - start_rgb[1]) * ratio)
                b = int(start_rgb[2] + (end_rgb[2] - start_rgb[2]) * ratio)
                draw.line([(x1, y1 + i), (x2, y1 + i)], fill=(r, g, b))

    def _parse_time(self, time_str):
        try:
            from datetime import datetime
            return datetime.strptime(time_str, "%H:%M")
        except Exception:
            return None

    def _get_break_info(self, current_lesson, next_lesson):
        timeend = current_lesson.get('timeend')
        next_timebegin = next_lesson.get('timebegin')
        if not timeend or not next_timebegin:
            return None, None, None, 0

        end_current = self._parse_time(timeend)
        start_next = self._parse_time(next_timebegin)
        if not end_current or not start_next:
            return None, None, None, 0

        duration = int((start_next - end_current).total_seconds() / 60)
        if duration == 20:
            return "big", timeend, next_timebegin, 20
        elif duration == 10:
            return "small", timeend, next_timebegin, 10
        return None, None, None, 0

    def generate_day_schedule_image(self, day, lessons, date_str, output_path='cache/schedule_day.png'):
        day_names = {'пн': 'Понедельник', 'вт': 'Вторник', 'ср': 'Среда', 'чт': 'Четверг', 'пт': 'Пятница',
                     'сб': 'Суббота'}
        day_name = day_names.get(day, day.upper())
        lesson_count = len(lessons)

        breaks_count = 0
        for i in range(len(lessons) - 1):
            b_type, _, _, dur = self._get_break_info(lessons[i], lessons[i + 1])
            if dur > 0:
                breaks_count += 1

        height = self.header_height + (lesson_count * self.row_height) + (
                    breaks_count * self.break_height) + self.padding * 2 + 60
        img = Image.new('RGB', (self.width, height), self.colors['background'])
        draw = ImageDraw.Draw(img)

        # Градиентный заголовок
        self._create_gradient_rect(draw, (0, 0, self.width, self.header_height),
                                   self.colors['header_start'], self.colors['header_end'])

        # Заголовок
        header_font = self._get_font(52, 'extrabold')
        title = f"{day_name} • {date_str}"
        bbox = draw.textbbox((0, 0), title, font=header_font)
        x_center = (self.width - (bbox[2] - bbox[0])) // 2

        draw.text((x_center + 3, 27), title, fill=self.text_shadow_rgb, font=header_font)
        draw.text((x_center, 24), title, fill=self.colors['header_text'], font=header_font)

        # Подзаголовок
        sub_font = self._get_font(32, 'bold')
        sub_title = f"({lesson_count} уроков)"
        sub_bbox = draw.textbbox((0, 0), sub_title, font=sub_font)
        sub_x = (self.width - sub_bbox[2]) // 2
        draw.text((sub_x, 84), sub_title, fill=self.colors['header_sub_text'], font=sub_font)

        y = self.header_height + self.padding
        for i, lesson in enumerate(lessons):
            row_color = self.colors['row_even'] if i % 2 == 0 else self.colors['row_odd']

            # Тень блока урока
            draw.rectangle([self.padding + 4, y + 4, self.width - self.padding + 4, y + self.row_height + 4],
                           fill=self.colors['shadow_rgb'])
            # Основной блок
            draw.rectangle([self.padding, y, self.width - self.padding, y + self.row_height],
                           fill=row_color, outline=self.colors['border'])
            # Декоративная полоска
            draw.rectangle([self.padding, y, self.padding + 10, y + self.row_height],
                           fill=self.colors['header_start'])

            # Время
            time_font = self._get_font(30, 'semibold')
            time_text = f"{lesson['timebegin']}–{lesson['timeend']}"
            draw.text((self.padding + 30, y + 24 + 8), time_text, fill=self.colors['time_text'], font=time_font)

            # Номер урока
            lesson_num = str(lesson['number'])
            num_font = self._get_font(32, 'bold')
            cx, cy = self.padding + 360, y + self.row_height // 2
            radius = 32
            draw.ellipse([cx - radius, cy - radius, cx + radius, cy + radius], fill=self.colors['header_start'])
            nw = draw.textbbox((0, 0), lesson_num, font=num_font)[2]
            nh = draw.textbbox((0, 0), lesson_num, font=num_font)[3]
            draw.text((cx - nw // 2 + 1, cy - nh // 2 - 1), lesson_num, fill='#FFFFFF', font=num_font)

            # Название (теперь включает тип занятия и кабинет в формате: "Название лаба/лекция -кабинет")
            name_font = self._get_font(34, 'semibold')  # Уменьшенный шрифт
            full_name = lesson['name']  # Уже содержит "Название лаба/лекция -кабинет"

            # Разделяем название на основную часть и суффикс (лаба/лекция -кабинет)
            # Формат: "Название предмета лаба -601" или "Название предмета лекция"
            suffix_match = re.search(r'\s+(лаба|лекция)(?:\s+-([\d-]+))?$', full_name)
            if suffix_match:
                base_name = full_name[:suffix_match.start()].strip()
                lesson_type = suffix_match.group(1)
                classroom = suffix_match.group(2) if suffix_match.group(2) else None

                # Рисуем основную часть названия
                max_width = self.width - self.padding * 2 - 550
                while draw.textbbox((0, 0), base_name, font=name_font)[2] > max_width and len(base_name) > 3:
                    base_name = base_name[:-4] + "..."
                draw.text((self.padding + 460, y + 22 + 10), base_name, fill=self.colors['text'], font=name_font)

                # Рисуем тип занятия и кабинет справа с выравниванием
                type_font = self._get_font(24, 'semibold')
                if classroom:
                    type_text = f"{lesson_type} -{classroom}"
                else:
                    type_text = lesson_type

                # Вычисляем позицию для правого выравнивания
                type_width = draw.textbbox((0, 0), type_text, font=type_font)[2]
                type_x = self.width - self.padding - type_width - 20
                type_y = y + 24 + 10
                draw.text((type_x, type_y), type_text, fill=self.colors['time_text'], font=type_font)
            else:
                # Если формат не распознан, рисуем как есть
                max_width = self.width - self.padding * 2 - 550
                name = full_name
                while draw.textbbox((0, 0), name, font=name_font)[2] > max_width and len(name) > 3:
                    name = name[:-4] + "..."
                draw.text((self.padding + 460, y + 22 + 10), name, fill=self.colors['text'], font=name_font)

            y += self.row_height

            # Перемены
            if i < len(lessons) - 1:
                b_type, b_start, b_end, dur = self._get_break_info(lesson, lessons[i + 1])

                if b_type == "big":
                    break_coords = [self.padding, y, self.width - self.padding, y + self.break_height]
                    # Тень
                    draw.rectangle([self.padding + 4, y + 4, self.width - self.padding + 4,
                                    y + self.break_height + 4],
                                   fill=self.colors['shadow_rgb'])
                    # Градиентный фон (горизонтальный)
                    break_start_rgb = self._hex_to_rgb(self.colors['break_bg_start'])
                    break_end_rgb = self._hex_to_rgb(self.colors['break_bg_end'])
                    self._create_gradient_rect(draw, break_coords,
                                               break_start_rgb, break_end_rgb, horizontal=True)
                    # Рамка
                    draw.rectangle(break_coords, outline='#FF8F00', width=2)

                    bt = f"Большая перемена: {b_start}–{b_end}"
                    break_font = self._get_font(24, 'semibold')
                    bw = draw.textbbox((0, 0), bt, font=break_font)[2]
                    # Изменено: было y + 2 + 12, стало y + 5 + 12 (на 3px ниже)
                    draw.text(((self.width - bw) // 2, y + 5 + 12), bt,
                              fill=self.colors['break_text'], font=break_font)
                    y += self.break_height

                elif b_type == "small":
                    sh = 40
                    break_coords = [self.padding, y + 1, self.width - self.padding, y + 1 + sh]
                    draw.rectangle([self.padding + 4, y + 1 + 4, self.width - self.padding + 4,
                                    y + 1 + sh + 4],
                                   fill=self.colors['shadow_rgb'])
                    draw.rectangle(break_coords, fill=self.colors['small_break'],
                                   outline=self.colors['border'], width=1)
                    bt = f"Перемена: {b_start}–{b_end}"
                    small_font = self._get_font(20, 'medium')
                    bw = draw.textbbox((0, 0), bt, font=small_font)[2]
                    # Изменено: было y + 1 + 8, стало y + 3 + 8 (на 2px ниже)
                    draw.text(((self.width - bw) // 2, y + 3 + 8), bt,
                              fill=self.colors['time_text'], font=small_font)
                    y += sh

        # Подпись
        footer_font = self._get_font(22, 'medium')
        draw.text((self.padding, y + 20), "Schedule Bot", fill=self.colors['time_text'], font=footer_font)

        img.save(output_path, quality=95, dpi=(300, 300))
        logger.info(f"Изображение сохранено: {output_path}")
        return output_path