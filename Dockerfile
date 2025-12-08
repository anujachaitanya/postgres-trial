FROM python:3.11-slim

WORKDIR /app

# Copy dependency files first
COPY pyproject.toml poetry.lock* ./

# Install poetry and dependencies (skip root package)
RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# Copy the rest of the application
COPY . .

# Install the root package now that files are present
RUN poetry install --no-interaction --no-ansi --only-root

CMD ["uvicorn", "postgres_trial.main:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]
