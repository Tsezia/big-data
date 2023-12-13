import json
import numpy
import requests
from requests.utils import quote
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime, timedelta
from transliterate import translit
from mrjob.protocol import JSONValueProtocol, TextValueProtocol
import sys

from schedule_grider import ScheduleGrider


class LecturerJob(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.lesson_json_to_lecturer_name_mapper),
            MRStep(mapper=self.lecturer_name_to_id_mapper), 
            MRStep(mapper=self.lecturer_id_to_weekly_schedule_mapper)
        ]

    def lesson_json_to_lecturer_name_mapper(self, _, line):
        lesson = json.loads(line)
        yield lesson, lesson["lecturer"]

    def lecturer_name_to_id_mapper(self, lesson, lecturer):

        link = f'https://rasp.omgtu.ru/api/search?term={quote(lecturer)}&type=person'
        response = requests.get(link, verify=True)
        if response.status_code == 200:
            lecturer_info = json.loads(response.text)
            lecturer_id = lecturer_info[0]['id']
        else:
            print("Не удалось получить данные о преподавателе " + lecturer)
            sys.exit()
        yield lesson, lecturer_id

    def lecturer_id_to_weekly_schedule_mapper(self, lesson, lecturer_id):

        start_time = self.return_week_monday_by_date(lesson["date"])
        weekly_schedules = {}

        while start_time != datetime(2024,1,1):
            link = f"https://rasp.omgtu.ru/api/schedule/person/{lecturer_id}?start={start_time.strftime('%Y.%m.%d')}&finish={(start_time + timedelta(days=5)).strftime('%Y.%m.%d')}&lng=1"
            response = requests.get(link, verify=True)
            if response.status_code == 200:
                weekly_schedule = json.loads(response.text)
                weekly_schedules[f"{start_time.strftime('%Y.%m.%d')} - {(start_time + timedelta(days=5)).strftime('%Y.%m.%d')}"] = weekly_schedule
            else:
                print("Не удалось получить данные о преподавателе c id " + lecturer_id)
                sys.exit()

            start_time += timedelta(days=7)
        
        scheduleGrider = ScheduleGrider(weekly_schedules)
        weekly_grids = scheduleGrider.transform_weekly_schedules_to_weekly_grids()
        weekly_grids = scheduleGrider.zeroing_slots_before_start_lesson(weekly_grids, lesson["date"], lesson["beginLesson"])

        yield lesson, weekly_grids
    
    def return_week_monday_by_date(self, lesson_day_str):
        lesson_day = datetime.strptime(lesson_day_str,  "%Y.%m.%d")
        lesson_weekday = lesson_day.weekday()
        week_monday = lesson_day - timedelta(days=lesson_weekday)

        return week_monday

if __name__ == '__main__':
    LecturerJob.run()