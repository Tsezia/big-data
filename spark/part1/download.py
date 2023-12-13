import datetime
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.parse import quote
import json

TEACHERS = ['Шарун', 'Тюменцев', 'Морарь', 'Чибикова']
TEACHER_URL = 'https://rasp.omgtu.ru/api/search?term=TEACHER_NAME&type=person'
SCHEDULE_URL = 'https://rasp.omgtu.ru/api/schedule/person/TEACHER_ID?start=START&finish=FINISH&lng=1'
START_DATE = datetime.date(2023, 9, 4)
END_DATE = datetime.date(2023, 12, 31)

def generate_interval(start, end):
    for n in range(int((end - start).days) // 7 + 1):
        yield start.strftime("%Y.%m.%d"), (start + datetime.timedelta(days = 6)).strftime("%Y.%m.%d")
        start = start + datetime.timedelta(days = 7)

def get_teacher_id(name):
    teacher_url = TEACHER_URL.replace('TEACHER_NAME', quote(name))
    try:
        response = urlopen(teacher_url)
        data = json.loads(response.read())
        return data[0]['id']
    except HTTPError as e:
        return None

for teacher in TEACHERS:
    teacher_id = get_teacher_id(teacher)
    if teacher_id is None:
        print(f"Teacher {teacher} not found")
        continue

    for interval in generate_interval(START_DATE, END_DATE):
        start_date, end_date = interval
        schedule_url = SCHEDULE_URL.replace('TEACHER_ID', str(teacher_id)).replace('START', start_date).replace('FINISH', end_date)
        try:
            response = urlopen(schedule_url)
            data = json.loads(response.read())
            if data:
                with open(f'files\\{teacher}-{start_date}-{end_date}.json', 'w') as f:
                    json.dump(data, f)
        except HTTPError as e:
            print(f"Error fetching schedule for {teacher} from {start_date} to {end_date}: {e}")



