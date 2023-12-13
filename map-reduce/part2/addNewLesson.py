import json

def add_new_lesson_for_rescheduling():
    print("Введите фамилию преподавателя:")
    lecturer = input()
    print("Введите название дисциплины: ")
    discipline = input()
    print("Введите название группы или подгруппы (или нескольких групп через пробел в случае, если это поток):")
    groups = input()
    print("Введите дату проведения пары в формате год.месяц.день:")
    date = input()
    print("Введите время начала пары:")
    beginLesson = input()
    
    new_lesson = str({"lecturer": lecturer, "discipline": discipline, "date": date, "beginLesson": beginLesson, "groups": groups.split()}).replace("'", '"')

    with open('part2/input/lessons.txt', 'a', encoding="utf8") as file:
        file.write(f"{new_lesson}\n")
    
    print("Запись произошла успешно!")

add_new_lesson_for_rescheduling()
