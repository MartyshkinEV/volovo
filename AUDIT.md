# AUDIT.md

## 1) Структура каталогов

Текущее дерево (по списку файлов):
- .gitignore
- main.py
- movement.py
- putevoy.html
- volovo/
  - main.py
  - movement.py
  - putevoy.html
  - volovo/
    - main.py
    - movement.py
    - putevoy.html

Наблюдения:
- Явное дублирование одинаковых файлов на трех уровнях: корень, volovo/, volovo/volovo/. Это создает риск расхождения версий и путаницу при импортах и запуске.
- Отсутствуют признаки пакетной структуры Python: нет __init__.py, нет pyproject.toml/setup.cfg/setup.py, нет requirements.txt.
- Шаблоны HTML (putevoy.html) лежат вперемежку с кодом, нет каталога templates/ и static/.
- Отсутствуют каталоги tests/, docs/, scripts/, .github/.
- main.py/movement.py встречаются в трех местах — непонятно какой из них «истинный» entrypoint.
- Имена файлов на английском, но доменная область (путевой лист) на русском; стоит унифицировать семантику и именование.

Рекомендованная базовая структура:
- pyproject.toml
- README.md, LICENSE
- .gitignore, .editorconfig
- src/volovo/
  - __init__.py
  - main.py (или cli.py)
  - movement.py
  - templates/putevoy.html
  - static/
- tests/
- .github/workflows/ci.yml

## 2) Конфигурация

Что есть:
- .gitignore — содержимое неизвестно.

Чего нет (и нужно добавить):
- Управление зависимостями и сборкой:
  - pyproject.toml (PEP 621) с зависимостями, минимальной версией Python, инструментами (black, isort, ruff/flake8, mypy, pytest).
  - requirements.txt/requirements-dev.txt при необходимости.
- Конфигурации инструментов качества кода:
  - ruff.toml или setup.cfg/pyproject для flake8/ruff.
  - mypy.ini.
  - pyproject.toml для black/isort.
  - .editorconfig.
- Конфигурация запуска:
  - .env.example и загрузка переменных окружения (python-dotenv), если есть параметры.
  - logging конфиг (стандартный logging с форматом и уровнями).
- Пакетирование:
  - Вариант entry point (console_scripts) для запуска через volovo в CLI.
- Шаблоны:
  - Перенести putevoy.html в templates/, при использовании Jinja2 — конфиг окружения шаблонов.

## 3) Безопасность

Наблюдения и риски (по структуре):
- HTML-файлы рядом с кодом: если они заполняются данными из внешних источников без экранирования — риск XSS. Рекомендовано использовать шаблонизатор (Jinja2 с autoescape=True) и отделение шаблонов.
- Нет явного управления зависимостями и их пиннинга — рискскачков версий/уязвимостей. Нужно pin/constraints + обновление через Dependabot.
- Отсутствуют тесты — высок риск регрессий, особенно при рефакторинге дублированных модулей.
- Не видно обработки путей/IO: при работе с файлами следует предотвращать directory traversal, проверять существование путей, режимы доступа, кодировку UTF-8.
- Нет конфигурации логирования — полезно централизованно логировать ошибки, без утечек чувствительных данных.
- Нет .env/.secrets — если в будущем появятся ключи/токены, их нужно вынести в окружение и защитить через .gitignore, добавить .env.example.

Рекомендации:
- Ввести зависимостный менеджмент и автоматическую проверку уязвимостей (pip-audit, safety) в CI.
- Использовать Jinja2 для генерации HTML с автоэкранированием.
- Добавить строгую валидацию и нормализацию входных данных (pydantic/attrs), если ввод планируется.
- Включить Content-Security-Policy для HTML, если файлы будут обслуживаться через веб.
- Логирование в файл с ротацией (RotatingFileHandler/TimedRotatingFileHandler) и уровнем INFO/ERROR.

## 4) GitHub/CI

Что отсутствует:
- .github/workflows/* — нет CI.
- Нет шаблонов issues/PR, CODEOWNERS, Dependabot.

Рекомендованный CI (GitHub Actions):
- Матрица Python: 3.10, 3.11, 3.12.
- Шаги:
  - set up python, cache pip.
  - pip install -e .[dev] или по requirements.
  - ruff/flake8 + black --check + isort --check.
  - mypy.
  - pytest с coverage (пример: pytest -q --cov=src --cov-report=xml).
  - pip-audit/safety.
- Загружать coverage в Codecov.
- dependabot.yml:
  - обновление GitHub Actions и pip еженедельно.
- Дополнительно:
  - pre-commit с хуками (black, isort, ruff, mixed-line-ending, trailing-whitespace).
  - CODEOWNERS.
  - PR template и Issue templates.

## 5) Best practices

Код/архитектура:
- Устранить дублирование: оставить один пакет volovo, удалить вложенные копии volovo/ и volovo/volovo/.
- Ввести src-layout (src/volovo) и __init__.py.
- Разделить слои:
  - cli.py (входная точка) или main.py с if __name__ == "__main__".
  - domain/infra/use-cases по необходимости.
  - movement.py как модуль доменной логики (переименовать более конкретно, напр. travel/movement_service.py).
- Переместить putevoy.html в templates/, использовать Jinja2 (autoescape, макросы, layout).
- Ввести типизацию (PEP 484) и mypy strict.
- Документирование: README.md, docstrings (Google/Numpy), краткая спецификация формата данных.
- Логирование через стандартный logging, без print.
- Детерминированность: фиксировать версии зависимостей, lock-файл (pip-tools/uv/pipenv/poetry).
- Кроссплатформенность путей: pathlib вместо os.path, не использовать абсолютные пути.
- Тесты:
  - pytest, покрытие ключевой логики.
  - тесты шаблонов (snapshot/DOM-проверки) при генерации HTML.

Процессы:
- Code review, линтеры и тесты в CI — обязательны для merge.
- Семантические версии и релизы (GitHub Releases).
- Changelog (Keep a Changelog).
- Лицензия (LICENSE) и указание авторства.

## 6) TODO улучшений

Приоритет P0:
- Удалить дубликаты файлов; оставить единственный корректный источник кода.
- Сформировать пакетную структуру: src/volovo с __init__.py.
- Перенести putevoy.html в src/volovo/templates/.
- Добавить pyproject.toml, зафиксировать минимальную версию Python, зависимости и инструменты (ruff/black/isort/mypy/pytest).
- Настроить базовый CI (lint + typecheck + tests).

Приоритет P1:
- Добавить README.md с описанием назначения, установки и запуска.
- Добавить тесты pytest для movement.py и рендеринга putevoy.html.
- Ввести Jinja2 с autoescape для генерации HTML.
- Настроить logging и базовую обработку ошибок.
- Добавить .editorconfig, pre-commit, ruff/mypy конфиги.
- Ввести requirements*.txt или использовать poetry/uv/pip-tools с lock-файлом.

Приоритет P2:
- Включить pip-audit/safety и Dependabot.
- Добавить LICENSE, CONTRIBUTING.md, CODEOWNERS, PR/Issue templates.
- Покрытие кода >80%, интеграция с Codecov.
- Улучшить именование модулей и структурирование доменной логики.
- Документация по структуре шаблонов и формату данных.

Приоритет P3:
- Автоматический релиз (semantic-release) и семантические версии.
- Сборка и публикация пакета (если уместно).
- Контейнеризация (Docker) для воспроизводимого окружения.
- Генерация документации (Sphinx/MkDocs).

Создано ботом ghbot.