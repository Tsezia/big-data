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
            MRStep(mapper=self.lecturer_name_to_id_mapper), 
            MRStep(mapper=self.lecturer_id_to_weekly_schedule_mapper)
        ]

    def lecturer_name_to_id_mapper(self, _, data):

        lecturer = json.loads(data)["lecturer"]
        link = f'https://rasp.omgtu.ru/api/search?term={quote(lecturer)}&type=person'
        response = requests.get(link, verify=True)
        if response.status_code == 200:
            lecturer_info = json.loads(response.text)
            lecturer_id = lecturer_info[0]['id']
        else:
            print("Не удалось получить данные о преподавателе " + lecturer)
            sys.exit()
        yield lecturer, lecturer_id


    def lecturer_id_to_weekly_schedule_mapper(self, lecturer_name, lecturer_id):
        start_time = datetime(2023,9,4)
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

        yield lecturer_name, weekly_grids
    

if __name__ == '__main__':
    LecturerJob.run()