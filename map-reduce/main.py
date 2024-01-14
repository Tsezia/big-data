import os

os.system(" python3 map-reduce/AuditoriumJob.py hdfs:///input/auditoriums.txt -r hadoop --output hdfs:///output/auditoriums_weekly_grids")
os.system(" python3 map-reduce/LecturerJob.py hdfs:///input/lessons.txt -r hadoop --output hdfs:///output/lecturers_weekly_grids")
os.system(" python3 map-reduce/GroupJob.py hdfs:///input/lessons.txt -r hadoop --output hdfs:///output/groups_weekly_grids")
os.system(" python part2/utils/compare_grids.py")
#os.system(" python part2/WeeklyGridsJob.py part2/output/compared_weekly_grids.txt")
