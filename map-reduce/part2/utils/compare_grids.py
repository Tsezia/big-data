import json
import os
from hdfs import InsecureClient

hdfs_client = InsecureClient("http://namenode:9870", user="root")

def collect_weekly_grids(hdfs_path):
    weekly_grids = []
    file_list = hdfs_client.list(hdfs_path)
    for file_name in file_list:
        hdfs_file_path = hdfs_path + "/" + file_name
        with hdfs_client.read(hdfs_file_path) as file:
            lines = file.readlines()
            for line in lines:
                weekly_grids.append(json.loads(line.decode('utf-8')))
    return weekly_grids

auditoriums_weekly_grids = collect_weekly_grids("/output/auditoriums_weekly_grids")
groups_weekly_grids = collect_weekly_grids("/output/groups_weekly_grids")
lecturers_weekly_grids = collect_weekly_grids("/output/lecturers_weekly_grids")

result_line = ""
for i in range(len(lecturers_weekly_grids)):
    compare_weekly_grids = {}
    compare_weekly_grids["group"] = groups_weekly_grids[i]
    compare_weekly_grids["lecturer"] = lecturers_weekly_grids[i]
    compare_weekly_grids["auditoriums"] = auditoriums_weekly_grids
    result_line += str(compare_weekly_grids).replace("'", '"')
    result_line += "\n"


with hdfs_client.write("/output/compared_weekly_grids.txt", encoding='utf-8') as file:
    file.write(result_line)

print("GRIDS COMPARED SUCCESSFUL")
