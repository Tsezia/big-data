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

    OUTPUT_PROTOCOL = TextValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.lesson_json_to_group_id_mapper), 
        ]

    def lesson_json_to_group_id_mapper(self, _, line):
        lesson = json.loads(line)
        yield _, line
    

if __name__ == '__main__':
    GroupJob.run()