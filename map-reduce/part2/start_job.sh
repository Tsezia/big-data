# Чистим input и output директории
hdfs dfs -rm -r /output/*
hdfs dfs -rm -r /input

# Загружаем входные данные в hdfs:
hdfs dfs -put map-reduce/input /

# Получаем сетки по каждой сущности
echo -e "\e[31m"
echo -e "\n----------------------------------------------------AUDITORIUMS JOB----------------------------------------------------\n"
python3 map-reduce/AuditoriumJob.py hdfs:///input/auditoriums.txt -r hadoop --output hdfs:///output/auditoriums_weekly_grids

echo -e "\e[33m"
echo -e "\n----------------------------------------------------LECTURERS JOB----------------------------------------------------\n"
python3 map-reduce/LecturerJob.py hdfs:///input/lessons.txt -r hadoop --output hdfs:///output/lecturers_weekly_grids

echo -e "\e[36m"
echo -e "\n----------------------------------------------------GROUPS JOB----------------------------------------------------\n"
python3 map-reduce/GroupJob.py hdfs:///input/lessons.txt -r hadoop --output hdfs:///output/groups_weekly_grids

echo -e "\e[0m"

# Переносим результаты в локальную папку для соединения
hadoop fs -get /output/auditoriums_weekly_grids map-reduce/output
hadoop fs -get /output/lecturers_weekly_grids map-reduce/output
hadoop fs -get /output/groups_weekly_grids map-reduce/output

# Соединяем полученные результаты
python3 map-reduce/utils/compare_grids.py

# Объединяем полученные матрицы и выводим ответ
echo -e "\e[32m"
echo -e "\n----------------------------------------------------GET RESULTS----------------------------------------------------\n"
python3 map-reduce/WeeklyGridsJob.py hdfs:///output/compared_weekly_grids.txt -r hadoop

echo -e "\e[0m"
