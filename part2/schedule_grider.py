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
