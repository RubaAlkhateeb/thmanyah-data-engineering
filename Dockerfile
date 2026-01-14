# Use Python 3.9 as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Pipenv
RUN pip install --no-cache-dir pipenv

# Copy Pipenv files first (better caching)
COPY Pipfile Pipfile.lock ./

# Install Python dependencies from Pipfile.lock
RUN pipenv install --system --deploy

# Copy all application code
COPY . .

# Set Python path so imports work
ENV PYTHONPATH=/app

# Command to run the application
CMD ["python", "main.py"]