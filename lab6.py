"""
Asynchronous ORM for working with YAML files.
Implements parser, metaclasses, descriptors, and logging.
"""

import asyncio
import functools
import logging
import os
import inspect  # Потрібен для перевірки асинхронності
import yaml  # Стандартна бібліотека для YAML (pip install pyyaml)
from typing import Any, Dict, List, Optional, Union

# Configuring logging
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
    """File not found exception."""

    def __init__(self, message: str = "File not found"):
        super().__init__(message)


class FileCorrupted(Exception):
    """File access error or corruption exception."""

    def __init__(self, message: str = "File corrupted or access error"):
        super().__init__(message)


def logged(exception_cls: type[Exception], mode: str = "console"):
    """Decorator for logging exceptions to console or file (supports sync and async)."""

    def decorator(func):
        # Логіка логування винесена в окрему функцію для уникнення дублювання
        def log_error(e, func_name):
            logger = logging.getLogger(func_name)
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
            logger.error(f"Exception in {func_name}: {e}")

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except exception_cls as e:
                log_error(e, func.__name__)
                raise e

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception_cls as e:
                log_error(e, func.__name__)
                raise e

        # Перевіряємо, чи функція асинхронна
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


class FileManager:
    """Class for reading and writing YAML files."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w', encoding='utf-8') as f:
                f.write("")

    @logged(FileCorrupted, mode="console")
    def read(self) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Reads and parses the YAML file."""
        msg = f"Reading file: {self.file_path}"
        logging.info(msg)
        file_ops_logger.info(msg)

        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                # Використання стандартного yaml модуля замість власного парсера
                data = yaml.safe_load(f)
                if data is None:
                    return {}
                return data
        except Exception as e:
            raise FileCorrupted(f"Failed to read file: {e}")

    @logged(FileCorrupted, mode="file")
    def write(self, data: Union[Dict, List]):
        """Writes data to the YAML file."""
        msg = f"Writing to file: {self.file_path}"
        logging.info(msg)
        file_ops_logger.info(msg)

        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                # Використання стандартного yaml модуля для запису
                yaml.dump(data, f, allow_unicode=True, default_flow_style=False)
        except Exception as e:
            raise FileCorrupted(f"Failed to write to file: {e}")


db_manager: Optional[FileManager] = None


class Field:
    """Base field descriptor."""

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
            raise ValueError(f"Field {self.name} must be an integer")
        if self.type_name == "TEXT" and not isinstance(value, str):
            raise ValueError(f"Field {self.name} must be a string")
        instance.__dict__[self.name] = value


class IntField(Field):
    """Integer field."""

    def __init__(self, pk: bool = False):
        super().__init__("INTEGER")
        self.pk = pk


class StringField(Field):
    """String field."""

    def __init__(self):
        super().__init__("TEXT")


class QuerySet:
    """Lazy query manager."""

    def __init__(self, model_class):
        self.model_class = model_class
        self.conditions = {}

    def filter(self, **kwargs):
        """Adds filter conditions."""
        self.conditions.update(kwargs)
        return self

    def __await__(self):
        return self.run_query().__await__()

    async def run_query(self):
        """Executes asynchronous query."""
        if db_manager is None:
            raise RuntimeError("DB Manager not initialized!")

        raw_data = await asyncio.to_thread(db_manager.read)

        if isinstance(raw_data, dict):
            # Якщо YAML повернув словник (наприклад, config), а ми очікуємо список
            raw_data = []

        result_objects = []

        # PyYAML повертає список словників або словник, адаптуємо:
        if raw_data is None:
            raw_data = []

        # Обробка даних, які можуть бути у форматі {"data": [...]} або просто [...]
        data_list = raw_data
        if isinstance(raw_data, dict) and "data" in raw_data:
            data_list = raw_data["data"]
        elif isinstance(raw_data, dict):
            # Якщо це просто словник, спробуємо перетворити його на список
            data_list = [raw_data]

        for record in data_list:
            if not isinstance(record, dict):
                continue

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
    """Metaclass for model field registration."""

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
    """Base ORM model."""

    fields: Dict[str, Field] = {}
    table_name: str = ""
    id: Optional[int] = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    async def create_table(cls):
        """Table initialization."""
        logging.info(f"Initializing table for {cls.__name__}")
        pass

    async def save(self):
        """Saves object to file."""
        if db_manager is None:
            raise RuntimeError("DB Manager not initialized!")

        all_data = await asyncio.to_thread(db_manager.read)

        # Адаптація під структуру PyYAML
        if isinstance(all_data, dict) and "data" in all_data:
            target_list = all_data["data"]
        elif isinstance(all_data, list):
            target_list = all_data
        else:
            target_list = []

        # Якщо ми прочитали порожній файл або словник, починаємо зі списку
        if not isinstance(target_list, list):
            target_list = []

        obj_data = {}
        for field in self.fields:
            val = getattr(self, field, None)
            if val is not None:
                obj_data[field] = val

        if not hasattr(self, 'id') or self.id is None:
            max_id = 0
            for row in target_list:
                if isinstance(row, dict) and 'id' in row and isinstance(row['id'], int):
                    if row['id'] > max_id:
                        max_id = row['id']
            self.id = max_id + 1
            obj_data['id'] = self.id

            target_list.append(obj_data)
        else:
            found = False
            for i, row in enumerate(target_list):
                if isinstance(row, dict) and row.get('id') == self.id:
                    target_list[i] = obj_data
                    found = True
                    break
            if not found:
                target_list.append(obj_data)

        # Якщо ми працюємо в режимі "data: [...]", зберігаємо структуру
        if isinstance(all_data, dict) and "data" in all_data:
            all_data["data"] = target_list
            write_data = all_data
        else:
            write_data = target_list

        await asyncio.to_thread(db_manager.write, write_data)


class User(Model):
    """User model."""
    id = IntField(pk=True)
    username = StringField()
    age = IntField()

    def __repr__(self):
        return f"{self.username}"


async def main():
    """Program entry point."""
    logging.info("--- Program Start ---")
    config_file = "orm_config.yaml"
    result_file = "result.yaml"
    ops_log_file = "file_operations.log"

    # Створюємо файли, якщо їх немає
    for f_name in [config_file, result_file, ops_log_file]:
        if not os.path.exists(f_name):
            logging.info(f"Creating file: {f_name}")
            with open(f_name, "w", encoding='utf-8') as f:
                if f_name == config_file:
                    # YAML формат для конфігу
                    f.write("database: students.yaml\n")
                else:
                    f.write("")

    try:
        config_manager = FileManager(config_file)
        config = config_manager.read()
    except FileNotFound:
        logging.error("Configuration file not found!")
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

    logging.info(f"Writing results to {result_file}")
    try:
        result_manager = FileManager(result_file)
        # Записуємо у форматі YAML
        result_manager.write({"data": values})
    except FileNotFound:
        logging.error("Result file not found!")

    logging.info("--- Program End ---")


if __name__ == "__main__":
    asyncio.run(main())