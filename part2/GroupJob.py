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

class GroupJob(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.lesson_json_to_groups_list_names_mapper), 
            MRStep(mapper=self.groups_list_names_to_groups_list_ids_mapper),
            MRStep(mapper=self.groups_list_ids_to_common_weekly_schedule_mapper),
        ]


    def lesson_json_to_groups_list_names_mapper(self, _, line):
        lesson = json.loads(line)
        yield lesson, lesson["groups"]
    

    def groups_list_names_to_groups_list_ids_mapper(self, lesson, groups_list_names):
        groups_list_ids = []
        for group_name in groups_list_names:
            processed_group_name = group_name.split("/")[0]
            link = f"https://rasp.omgtu.ru/api/search?term={processed_group_name}&type=group"
            response = requests.get(link, verify=True)
            if response.status_code == 200:
                group_info = json.loads(response.text)
                groups_list_ids.append(group_info[0]['id'])
            else:
                print("Не удалось получить данные о группе " + processed_group_name)
                sys.exit()
        yield lesson, groups_list_ids
    

    def groups_list_ids_to_common_weekly_schedule_mapper(self, lesson, groups_list_ids):

        start_time = self.return_week_monday_by_date(lesson["date"])

        weekly_schedules_list = [{} for _ in range(len(groups_list_ids))]

        while start_time != datetime(2024,1,1):
            for i in range(len(groups_list_ids)):
                result_group_weekly_schedule = []
                link = f"https://rasp.omgtu.ru/api/schedule/group/{groups_list_ids[i]}?start={start_time.strftime('%Y.%m.%d')}&finish={(start_time + timedelta(days=5)).strftime('%Y.%m.%d')}&lng=1"
                response = requests.get(link, verify=True)
                if response.status_code == 200:
                    group_weekly_schedule = json.loads(response.text)
                    if "/" in lesson["groups"][i]:
                        for group_lesson in group_weekly_schedule:
                            if group_lesson["subGroup"] == None or group_lesson["subGroup"] == lesson["groups"][i]:
                                result_group_weekly_schedule.append(group_lesson)
                    else:
                        result_group_weekly_schedule.extend(group_weekly_schedule)
                    
                    weekly_schedules_list[i][f"{start_time.strftime('%Y.%m.%d')} - {(start_time + timedelta(days=5)).strftime('%Y.%m.%d')}"] = result_group_weekly_schedule
                    
                else:
                    print("Не удалось получить данные о группе c id " + groups_list_ids[i])
                    sys.exit()

            start_time += timedelta(days=7)

        weekly_grids_list = []
        
        for weekly_schedules in weekly_schedules_list:
            scheduleGrider = ScheduleGrider(weekly_schedules)
            weekly_grids = scheduleGrider.transform_weekly_schedules_to_weekly_grids()

            weekly_grids = scheduleGrider.filter_weekly_grids_by_max_number_lessons(weekly_grids, 4) # Фильтр по максимальному количеству пар в день
            weekly_grids = scheduleGrider.filter_weekly_grids_by_existing_windows(weekly_grids) # Фильтр, не позволяющий переносимой паре образовать окно в расписании

            weekly_grids_list.append(scheduleGrider.zeroing_slots_before_start_lesson(weekly_grids, lesson["date"], lesson["beginLesson"]))
        
        merged_weekly_grids = scheduleGrider.merge_weekly_grids(weekly_grids_list)
        
        merged_weekly_grids["lesson"] = lesson

        yield lesson, merged_weekly_grids



    def return_week_monday_by_date(self, lesson_day_str):
        lesson_day = datetime.strptime(lesson_day_str,  "%Y.%m.%d")
        lesson_weekday = lesson_day.weekday()
        week_monday = lesson_day - timedelta(days=lesson_weekday)

        return week_monday

            
if __name__ == '__main__':
    GroupJob.run()