import json
import numpy
import requests
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime, timedelta
from transliterate import translit
from mrjob.protocol import JSONValueProtocol, TextValueProtocol
import sys

from schedule_grider import ScheduleGrider

class Rescheduling(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.auditoriums_name_to_auditorium_id_mapper), 
            MRStep(mapper=self.auditoriums_id_to_auditorium_weekly_schedule_mapper)
        ]

    def auditoriums_name_to_auditorium_id_mapper(self, _, auditorium):
        link = f"https://rasp.omgtu.ru/api/search?term={auditorium}&type=auditorium"
        response = requests.get(link, verify=True)
        if response.status_code == 200:
            auditorium_info = json.loads(response.text)
            auditorium_id = auditorium_info[0]['id']
        else:
            print("Не удалось получить данные об аудитории " + auditorium_name)
            sys.exit()
        yield auditorium, auditorium_id


    def auditoriums_id_to_auditorium_weekly_schedule_mapper(self, auditorium_name, auditorium_id):
        start_time = datetime(2023,9,4)
        weekly_schedules = {}

        while start_time != datetime(2024,1,1):
            link = f"https://rasp.omgtu.ru/api/schedule/auditorium/{auditorium_id}?start={start_time.strftime('%Y.%m.%d')}&finish={(start_time + timedelta(days=5)).strftime('%Y.%m.%d')}&lng=1"
            response = requests.get(link, verify=True)
            if response.status_code == 200:
                weekly_schedule = json.loads(response.text)
                weekly_schedules[f"{start_time.strftime('%Y.%m.%d')} - {(start_time + timedelta(days=5)).strftime('%Y.%m.%d')}"] = weekly_schedule
            else:
                print("Не удалось получить данные об аудитории c id " + auditorium_id)
                sys.exit()

            start_time += timedelta(days=7)
        
        scheduleGrider = ScheduleGrider(weekly_schedules)
        weekly_grids = scheduleGrider.transform_weekly_schedules_to_weekly_grids()

        yield auditorium_name, weekly_grids
    

if __name__ == '__main__':
    Rescheduling.run()