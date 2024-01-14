import json
import numpy as np
import requests
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime, timedelta, time
from transliterate import translit
from mrjob.protocol import JSONValueProtocol, TextValueProtocol
import sys


class ScheduleGrider():

    def __init__(self, weekly_schedules=None):

        self.date_format = "%Y.%m.%d"
        self.weekly_schedules = weekly_schedules
    
    def transform_weekly_schedules_to_weekly_grids(self):
        result = {}
        for key in self.weekly_schedules.keys():
            monday = datetime.strptime(key.split(" - ")[0], self.date_format)
            result[key] = self.transform_weekly_schedule_to_weekly_grid(self.weekly_schedules[key], monday)
        return result

    def transform_weekly_schedule_to_weekly_grid(self, weekly_schedule, monday):
        weekly_grid = np.ones((6, 6))
        for lesson in weekly_schedule:
            lessonDay = datetime.strptime(lesson["date"], self.date_format)

            beginLessonTime = datetime.strptime(lesson["beginLesson"], "%H:%M").time()
            endLessonTime = datetime.strptime(lesson["endLesson"], "%H:%M").time()

            day_index = (lessonDay - monday).days

            if beginLessonTime <= time(8, 0) <= endLessonTime or beginLessonTime <= time(9, 30) <= endLessonTime:
                weekly_grid[day_index][0] = 0

            if beginLessonTime <= time(9, 40) <= endLessonTime or beginLessonTime <= time(11, 10) <= endLessonTime:
                weekly_grid[day_index][1] = 0

            if beginLessonTime <= time(11, 35) <= endLessonTime or beginLessonTime <= time(13, 5) <= endLessonTime:
                weekly_grid[day_index][2] = 0

            if beginLessonTime <= time(13, 15) <= endLessonTime or beginLessonTime <= time(14, 45) <= endLessonTime:
                weekly_grid[day_index][3] = 0

            if beginLessonTime <= time(15, 10) <= endLessonTime or beginLessonTime <= time(16, 40) <= endLessonTime:
                weekly_grid[day_index][4] = 0
            
            if beginLessonTime <= time(16, 50) <= endLessonTime or beginLessonTime <= time(18, 20) <= endLessonTime:
                weekly_grid[day_index][5] = 0
            
        return weekly_grid.tolist()
    

    def zeroing_slots_before_start_lesson(self, weekly_grids, lesson_day_str, lesson_time):

        updated_weekly_grids = weekly_grids

        time_indexes = {"08:00": 0, "9:40": 1, "11:35": 2, "13:15": 3, "15:10": 4, "16:50": 5}
        lesson_day = datetime.strptime(lesson_day_str,  "%Y.%m.%d")
        week_monday = lesson_day - timedelta(days=lesson_day.weekday())

        current_week_key = "{} - {}".format((week_monday).strftime('%Y.%m.%d'), (week_monday + timedelta(days=5)).strftime('%Y.%m.%d'))

        for i in range(len(updated_weekly_grids[current_week_key])):
            for j in range(len(updated_weekly_grids[current_week_key][i])):
                updated_weekly_grids[current_week_key][i][j] = 0.0
                if i == lesson_day.weekday() and j == time_indexes[lesson_time]:
                    return updated_weekly_grids
        
        return updated_weekly_grids
    

    def get_free_slot_description_by_compared_scheduling(self, group_weekly_grids, lecturer_weekly_grids, auditorium_weekly_grids):

        time_indexes = {0: "08:00", 1: "9:40", 2: "11:35", 3: "13:15", 4: "15:10", 5: "16:50"}

        lesson_data = group_weekly_grids["lesson"]
        auditorium_name = auditorium_weekly_grids["auditorium_name"]

        synchronized_auditorium_weekly_grids = self.get_synchonized_weekly_grids(group_weekly_grids, auditorium_weekly_grids)
        merged_weekly_grids = self.merge_weekly_grids([synchronized_auditorium_weekly_grids, group_weekly_grids, lecturer_weekly_grids])

        free_slot_description = ""

        for key in sorted(merged_weekly_grids.keys()):
            for i in range(len(merged_weekly_grids[key])):
                for j in range(len(merged_weekly_grids[key][i])):
                    if merged_weekly_grids[key][i][j] == 1.0:
                        free_slot_monday = datetime.strptime(key.split(" - ")[0], "%Y.%m.%d")
                        free_slot_day = free_slot_monday + timedelta(days=i)
                        free_slot_start_lesson_time = datetime.strptime(time_indexes[j],  "%H:%M").time()
                        free_slot_end_lesson_time = (datetime.combine(datetime.today(), free_slot_start_lesson_time) + timedelta(hours=1, minutes=30)).time()

                        free_slot_description = ""
                        free_slot_description += "------------------------------------------------------------\n"
                        free_slot_description += "ИНФОРМАЦИЯ О ПЕРЕНОСИМОЙ ПАРЕ \n"
                        free_slot_description += "Название дисциплины: {}\n".format(lesson_data['discipline'])
                        free_slot_description += "Задействованные группы: {}\n".format(', '.join(lesson_data['groups']))
                        free_slot_description += "Преподаватель: {}\n".format(lesson_data['lecturer'])
                        free_slot_description += "Дата и начало проведения пары: {} {}\n".format(lesson_data['date'], lesson_data['beginLesson'])
                        free_slot_description += "\nСВОБОДНЫЙ СЛОТ ДЛЯ ПРОВЕДЕНИЯ ПАРЫ\n"
                        free_slot_description += "Аудитория, в которой можно провести пару: {}\n".format(auditorium_name)
                        free_slot_description += "Дата проведения пары: {}\n".format(free_slot_day.strftime('%Y.%m.%d'))
                        free_slot_description += "Время проведения пары: {} - {}\n".format(free_slot_start_lesson_time.strftime('%H:%M'), free_slot_end_lesson_time.strftime('%H:%M'))
                        free_slot_description += "------------------------------------------------------------\n \n"

                        return free_slot_description
                        

        
        return free_slot_description
        
        
    def get_synchonized_weekly_grids(self, example_weekly_grids, processed_weekly_grids):
        synchronized_weekly_grids = {}
        for key in example_weekly_grids.keys():
            if key not in ["lesson", "auditorium_name"]:
                synchronized_weekly_grids[key] = processed_weekly_grids[key]
        return synchronized_weekly_grids
    

    def merge_weekly_grids(self, weekly_grids_list):
        merged_weekly_grids = {}
        start_weekly_grids = weekly_grids_list[0]
        for key in start_weekly_grids.keys():
            if key not in ["lesson", "auditorium_name"]:
                result = np.array(start_weekly_grids[key])
                for weekly_grids in weekly_grids_list[1:]:
                    weekly_grid = np.array(weekly_grids[key])
                    result = np.where(weekly_grid == 0, weekly_grid, result)
                merged_weekly_grids[key] = result.tolist()
        return merged_weekly_grids


    def filter_weekly_grids_by_max_number_lessons(self, weekly_grids, max_number):
        filtered_weekly_grids = weekly_grids
        for key in weekly_grids.keys():
            for i in range(len(weekly_grids[key])):
                num_zeros = np.count_nonzero(np.array(filtered_weekly_grids[key][i]) == 0.0)
                if num_zeros >= max_number:
                    filtered_weekly_grids[key][i] = np.zeros(6)
        return filtered_weekly_grids
    

    def filter_weekly_grids_by_existing_windows(self, weekly_grids):
        filtered_weekly_grids = weekly_grids
        for key in weekly_grids.keys():
            for i in range(len(weekly_grids[key])):

                arr = np.array(filtered_weekly_grids[key][i])

                first_zero_index = np.where(arr == 0.0)[0]
                last_zero_index = np.where(arr == 0.0)[0]

                needless_indexes = []
                if first_zero_index.size > 0 and first_zero_index[0] > 1:
                    needless_indexes.extend(list(range(0, first_zero_index[0] - 1)))
                if last_zero_index.size > 0 and last_zero_index[-1] < 4:
                    needless_indexes.extend(list(range(last_zero_index[-1] + 2, 6)))

                for needless_index in needless_indexes:
                    filtered_weekly_grids[key][i][needless_index] = 0.0

        return filtered_weekly_grids


class WeeklyGridsJob(MRJob):

    OUTPUT_PROTOCOL = TextValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.compared_weekly_grids_to_free_slots_descriptions_mapper), 
        ]

    def compared_weekly_grids_to_free_slots_descriptions_mapper(self, _, compared_weekly_grids_str):

        scheduleGrider = ScheduleGrider()

        compared_weekly_grids = json.loads(compared_weekly_grids_str)
        free_slot_description = "Свободные слоты для переноса пары до конца семестра не обнаружены"
        for auditorium_weekly_grid in compared_weekly_grids["auditoriums"]:
            free_slot_description = scheduleGrider.get_free_slot_description_by_compared_scheduling(compared_weekly_grids["group"],
                                                                            compared_weekly_grids["lecturer"],
                                                                            auditorium_weekly_grid)
            if free_slot_description != "": break

        yield "_", free_slot_description
    

if __name__ == '__main__':
    WeeklyGridsJob.run()
