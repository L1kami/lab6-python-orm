# lab6-python-orm
Asynchronous ORM implementation for text files using Python (Metaclasses, Descriptors, Asyncio).
# Async File ORM

Асинхронна ORM для роботи з текстовими файлами (YAML-like формат). Реалізує власний парсер, метакласи для реєстрації моделей, дескриптори для типізації полів та систему логування.

## Особливості

* **Asynchronous:** Використовує `asyncio` для неблокуючих операцій вводу/виводу.
* **Custom ORM:** Реалізація патерну Active Record через метакласи (`ModelMeta`).
* **Strong Typing:** Дескриптори (`IntField`, `StringField`) контролюють типи даних.
* **Logging:** Декоратор `@logged` для гнучкого логування помилок у консоль або файл.
* **Lazy QuerySet:** Відкладене виконання запитів (`await User.objects.filter(...)`).

## Використання

1. **Встановлення:**
   Потрібен Python 3.8+. Зовнішні бібліотеки не використовуються.

2. **Запуск:**
   ```bash
   python lab6.py
   ```

3. **Приклад коду:**
   ```python
   class User(Model):
       id = IntField(pk=True)
       username = StringField()
       age = IntField()

   # Створення та збереження
   user = User(username="Andriy", age=25)
   await user.save()

   # Пошук
   users = await User.objects.filter(age=25)
   ```

## Структура файлів (Generated)
При запуску програма створить:
* `app.log` - загальні логи.
* `file_operations.log` - логи операцій з файлами.
* `students.yaml` - файл бази даних.
* `result.yaml` - результати вибірки.
