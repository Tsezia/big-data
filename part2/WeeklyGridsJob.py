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

class WeeklyGridsJob(MRJob):

    OUTPUT_PROTOCOL = TextValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.compared_weekly_grids_to_free_slots_mapper), 
        ]

    def compared_weekly_grids_to_free_slots_mapper(self, _, compared_weekly_grids_str):

        scheduleGrider = ScheduleGrider()

        compared_weekly_grids = json.loads(compared_weekly_grids_str)
        for auditorium_weekly_grid in compared_weekly_grids["auditoriums"]:
            free_slot = scheduleGrider.get_free_slot_by_compared_scheduling(compared_weekly_grids["group"],
                                                                            compared_weekly_grids["lecturer"],
                                                                            auditorium_weekly_grid)

        yield "Что то", str(free_slot)
    

if __name__ == '__main__':
    WeeklyGridsJob.run()