import json

auditoriums_weekly_grids = []
groups_weekly_grids = []
lecturers_weekly_grids = []


with open("part2/output/auditoriums_weekly_grids.txt", "r", encoding="utf8") as file:
    lines = file.readlines()
    for line in lines:
        auditoriums_weekly_grids.append(json.loads(line))


with open("part2/output/groups_weekly_grids.txt", "r", encoding="utf8") as file:
    lines = file.readlines()
    for line in lines:
        groups_weekly_grids.append(json.loads(line))


with open("part2/output/lecturers_weekly_grids.txt", "r", encoding="utf8") as file:
    lines = file.readlines()
    for line in lines:
       lecturers_weekly_grids.append(json.loads(line))

result_line = ""
for i in range(len(lecturers_weekly_grids)):
    compare_weekly_grids = {}
    compare_weekly_grids["group"] = groups_weekly_grids[i]
    compare_weekly_grids["lecturer"] = lecturers_weekly_grids[i]
    compare_weekly_grids["auditoriums"] = auditoriums_weekly_grids
    result_line += str(compare_weekly_grids).replace("'", '"')
    result_line += "\n"

with open('part2/output/compared_weekly_grids.txt', 'w', encoding="utf8") as file:
    file.write(result_line)

print("GRIDS COMPARED SUCCESSFUL")




