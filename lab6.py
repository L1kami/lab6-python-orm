"""
Асинхронна ORM для роботи з текстовими файлами.
Реалізує парсер, метакласи, дескриптори та логування.
"""

import asyncio
import functools
import logging
import os
from typing import Any, Dict, List, Optional, Union

logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    encoding='utf-8'
)

file_ops_logger = logging.getLogger("FileOperations")
file_ops_logger.setLevel(logging.INFO)
file_ops_handler = logging.FileHandler("file_operations.log", encoding='utf-8')
file_ops_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
file_ops_logger.addHandler(file_ops_handler)


class FileNotFound(Exception):
    """Файл не знайдено."""
    def __init__(self, message: str = "Файл не знайдено"):
        super().__init__(message)


class FileCorrupted(Exception):
    """Помилка доступу або пошкодження файлу."""
    def __init__(self, message: str = "Файл пошкоджено або помилка доступу"):
        super().__init__(message)


def logged(exception_cls: type[Exception], mode: str = "console"):
    """Декоратор для логування винятків у консоль або файл."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception_cls as e:
                logger = logging.getLogger(func.__name__)
                logger.setLevel(logging.ERROR)

                if logger.hasHandlers():
                    logger.handlers.clear()

                if mode == "file":
                    handler = logging.FileHandler(
                        "file_operations.log", encoding='utf-8'
                    )
                    fmt = '%(asctime)s - %(levelname)s - %(message)s'
                    formatter = logging.Formatter(fmt)
                else:
                    handler = logging.StreamHandler()
                    fmt = '[CONSOLE LOG] %(levelname)s: %(message)s'
                    formatter = logging.Formatter(fmt)

                handler.setFormatter(formatter)
                logger.addHandler(handler)

                logger.error(f"Виняток у {func.__name__}: {e}")

                raise e
        return wrapper
    return decorator


class FileManager:
    """Клас для читання та запису файлів."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w', encoding='utf-8') as f:
                f.write("")

    @staticmethod
    def _parse_value(value: str) -> Union[int, str]:
        """Конвертує рядок у число, якщо це можливо."""
        value = value.strip()
        if value.isdigit():
            return int(value)
        return value

    @logged(FileCorrupted, mode="console")
    def read(self) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Читає та парсить файл."""
        msg = f"Читання файлу: {self.file_path}"
        logging.info(msg)
        file_ops_logger.info(msg)

        data = []
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            if not lines:
                return {}

            first_line = lines[0].strip()

            if first_line.startswith("data:"):
                res_list = []
                for line in lines[1:]:
                    if line.strip().startswith("- "):
                        res_list.append(line.strip()[2:])
                return {"data": res_list}

            if first_line.startswith("- "):
                current_obj = {}
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue

                    if line.startswith("- "):
                        if current_obj:
                            data.append(current_obj)
                        current_obj = {}
                        key_part = line[2:]
                        if ":" in key_part:
                            k, v = key_part.split(":", 1)
                            current_obj[k.strip()] = self._parse_value(v)
                    elif ":" in line:
                        k, v = line.split(":", 1)
                        current_obj[k.strip()] = self._parse_value(v)
                if current_obj:
                    data.append(current_obj)
                return data

            config = {}
            for line in lines:
                if ":" in line:
                    k, v = line.split(":", 1)
                    config[k.strip()] = self._parse_value(v)
            return config

        except Exception as e:
            raise FileCorrupted(f"Не вдалося прочитати файл: {e}")

    @logged(FileCorrupted, mode="file")
    def write(self, data: Union[Dict, List]):
        """Записує дані у файл."""
        msg = f"Запис у файл: {self.file_path}"
        logging.info(msg)
        file_ops_logger.info(msg)

        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                if isinstance(data, dict):
                    for k, v in data.items():
                        f.write(f"{k}:\n")
                        if isinstance(v, list):
                            for item in v:
                                f.write(f"- {item}\n")
                        else:
                            f.write(f" {v}\n")
                elif isinstance(data, list):
                    for record in data:
                        keys = list(record.keys())
                        if not keys:
                            continue
                        f.write(f"- {keys[0]}: {record[keys[0]]}\n")
                        for k in keys[1:]:
                            f.write(f"  {k}: {record[k]}\n")
        except Exception as e:
            raise FileCorrupted(f"Не вдалося записати у файл: {e}")


db_manager: Optional[FileManager] = None


class Field:
    """Базовий дескриптор поля."""

    def __init__(self, type_name: str):
        self.type_name = type_name
        self.name = ""

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.__dict__.get(self.name)

    def __set__(self, instance, value):
        if self.type_name == "INTEGER" and not isinstance(value, int):
            raise ValueError(f"Поле {self.name} має бути числом")
        if self.type_name == "TEXT" and not isinstance(value, str):
            raise ValueError(f"Поле {self.name} має бути рядком")
        instance.__dict__[self.name] = value


class IntField(Field):
    """Числове поле."""
    def __init__(self, pk: bool = False):
        super().__init__("INTEGER")
        self.pk = pk


class StringField(Field):
    """Рядкове поле."""
    def __init__(self):
        super().__init__("TEXT")


class QuerySet:
    """Менеджер лінивих запитів."""

    def __init__(self, model_class):
        self.model_class = model_class
        self.conditions = {}

    def filter(self, **kwargs):
        """Додає умови фільтрації."""
        self.conditions.update(kwargs)
        return self

    def __await__(self):
        return self.run_query().__await__()

    async def run_query(self):
        """Виконує асинхронний запит."""
        if db_manager is None:
            raise RuntimeError("Менеджер БД не ініціалізовано!")

        raw_data = await asyncio.to_thread(db_manager.read)

        if isinstance(raw_data, dict):
            raw_data = []

        result_objects = []

        for record in raw_data:
            match = True
            for key, val in self.conditions.items():
                if record.get(key) != val:
                    match = False
                    break

            if match:
                obj = self.model_class()
                for field in self.model_class.fields:
                    if field in record:
                        setattr(obj, field, record[field])
                result_objects.append(obj)

        return result_objects


class ModelMeta(type):
    """Метаклас для реєстрації полів моделі."""

    def __new__(cls, name, bases, attrs):
        if name == "Model":
            return super().__new__(cls, name, bases, attrs)

        fields = {}
        for k, v in attrs.items():
            if isinstance(v, Field):
                fields[k] = v

        attrs['fields'] = fields
        attrs['table_name'] = name.lower() + "s"

        return super().__new__(cls, name, bases, attrs)

    @property
    def objects(cls):
        return QuerySet(cls)


class Model(metaclass=ModelMeta):
    """Базова модель ORM."""

    fields: Dict[str, Field] = {}
    table_name: str = ""
    id: Optional[int] = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    async def create_table(cls):
        """Ініціалізація таблиці."""
        logging.info(f"Ініціалізація таблиці для {cls.__name__}")
        pass

    async def save(self):
        """Зберігає об'єкт у файл."""
        if db_manager is None:
            raise RuntimeError("Менеджер БД не ініціалізовано!")

        all_data = await asyncio.to_thread(db_manager.read)
        if isinstance(all_data, dict):
            all_data = []

        obj_data = {}
        for field in self.fields:
            val = getattr(self, field, None)
            if val is not None:
                obj_data[field] = val

        if not hasattr(self, 'id') or self.id is None:
            max_id = 0
            for row in all_data:
                if 'id' in row and isinstance(row['id'], int):
                    if row['id'] > max_id:
                        max_id = row['id']
            self.id = max_id + 1
            obj_data['id'] = self.id

            all_data.append(obj_data)
        else:
            found = False
            for i, row in enumerate(all_data):
                if row.get('id') == self.id:
                    all_data[i] = obj_data
                    found = True
                    break
            if not found:
                all_data.append(obj_data)

        await asyncio.to_thread(db_manager.write, all_data)


class User(Model):
    """Модель користувача."""
    id = IntField(pk=True)
    username = StringField()
    age = IntField()

    def __repr__(self):
        return f"{self.username}"


async def main():
    """Точка входу в програму."""
    logging.info("--- Запуск програми ---")
    config_file = "orm_config.yaml"
    result_file = "result.yaml"
    ops_log_file = "file_operations.log"

    for f_name in [config_file, result_file, ops_log_file]:
        if not os.path.exists(f_name):
            logging.info(f"Створення файлу: {f_name}")
            with open(f_name, "w", encoding='utf-8') as f:
                if f_name == config_file:
                    f.write("database: students.yaml\n")
                else:
                    f.write("")

    try:
        config_manager = FileManager(config_file)
        config = config_manager.read()
    except FileNotFound:
        logging.error("Конфігураційний файл не знайдено!")
        return

    if not isinstance(config, dict):
        config = {}

    db_name = config.get("database", "students.yaml")

    global db_manager
    db_manager = FileManager(db_name)

    await User.create_table()

    new_users = [
        User(username="Andriy", age=20),
        User(username="Ivan", age=20),
        User(username="Maria", age=20),
        User(username="Petro", age=20),
        User(username="Oksana", age=20)
    ]

    for u in new_users:
        await u.save()

    users = await User.objects.filter(age=20)

    values = [str(u) for u in users]

    print("data:")
    for val in values:
        print(f"- {val}")

    logging.info(f"Запис результатів у {result_file}")
    try:
        result_manager = FileManager(result_file)
        result_manager.write({"data": values})
    except FileNotFound:
        logging.error("Файл результатів не знайдено!")

    logging.info("--- Завершення програми ---")


if __name__ == "__main__":
    asyncio.run(main())
