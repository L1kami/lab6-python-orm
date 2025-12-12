# Lab 6: Asynchronous Python ORM (YAML)

This project implements a custom **Asynchronous Object-Relational Mapping (ORM)** system that uses **YAML files** as a database storage format. It demonstrates advanced Python concepts such as metaclasses, descriptors, decorators, and asynchronous programming with `asyncio`.

## 📋 Features

- **Asynchronous I/O**: All file operations (read/write) are non-blocking using `asyncio`.
- **Custom ORM**:
  - `Model` class with metaclass-based field registration.
  - `Field` descriptors for data validation (`IntField`, `StringField`).
  - `QuerySet` for filtering data.
- **YAML Support**: Uses `PyYAML` for reliable parsing and serialization.
- **Logging**: Custom `@logged` decorator that handles exceptions and logs operations to both console and file.
- **Robust Error Handling**: Custom exceptions (`FileNotFound`, `FileCorrupted`).

## 🛠️ Requirements

- Python 3.8+
- [PyYAML](https://pypi.org/project/PyYAML/)

## 🚀 Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone <your-repo-link>
   cd <your-repo-folder>