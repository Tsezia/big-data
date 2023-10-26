import os

os.system(" python part2/AuditoriumJob.py part2/input/auditoriums.txt > part2/output/auditoriums_weekly_grids.txt")
os.system(" python part2/GroupJob.py part2/input/lessons.txt > part2/output/groups_weekly_grids.txt")
os.system(" python part2/LecturerJob.py part2/input/lessons.txt > part2/output/lecturers_weekly_grids.txt")
os.system(" python part2/utils/compare_grids.py")
os.system(" python part2/WeeklyGridsJob.py part2/output/compared_weekly_grids.txt")
