import numpy as np
import requests
import json
from datetime import datetime, timedelta, time
from transliterate import translit

class ScheduleGrider():

    def __init__(self, weekly_schedules):

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

        current_week_key = f"{(week_monday).strftime('%Y.%m.%d')} - {(week_monday + timedelta(days=5)).strftime('%Y.%m.%d')}"

        flag = False
        for i in range(len(updated_weekly_grids[current_week_key])):
            for j in range(len(updated_weekly_grids[current_week_key][i])):
                updated_weekly_grids[current_week_key][i][j] = 0.0
                if i == lesson_day.weekday() and j == time_indexes[lesson_time]:
                    return updated_weekly_grids
        
        return updated_weekly_grids


