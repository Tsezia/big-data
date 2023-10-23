import json

def add_new_lesson_for_rescheduling():
    print("Введите фамилию преподавателя:")
    lecturer = input()
    print("Введин название дисциплины: ")
    discipline = input()
    print("Введите название группы или подгруппы (или нескольких групп через пробел в случае, если это поток):")
    group = input()
    print("Введите дату проведения пары в формате год.месяц.день:")
    date = input()
    print("Введите время начала пары:")
    beginLesson = input()

    with open('Laba2/part2/input/lessons.json', 'r', encoding="utf8") as file:
        lessons = json.load(file)
    
    new_lesson = {"lecturer": lecturer, "discipline": discipline, "date": date, "beginLesson": beginLesson}
    if " " in group:
        groups = group.split()
        new_lesson["stream"] = groups
    elif '/' in group:
        new_lesson["subgroup"] = group
    else:
        new_lesson["group"] = group
    
    lessons.append(new_lesson)
    updated_json = json.dumps(lessons, ensure_ascii=False)

    with open('Laba2/part2/input/lessons.json', 'w', encoding="utf8") as file:
        file.write(updated_json)
    
    print("Запись произошла успешно!")

add_new_lesson_for_rescheduling()
