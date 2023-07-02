# Dockerfile

# Base image
FROM python:3.9-slim-buster

# Set working directory
WORKDIR /app

# Copy project files to working directory
COPY requirements.txt requirements.txt
RUN apt-get update \
    && apt-get clean

# Install dependencies
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt

RUN apt-get update \
    &&  apt-get install -y \
        build-essential \
        cmake \
        libsasl2-dev \
        curl

RUN pip install -U pip

# Poetry installation
RUN pip install poetry==1.1.15
ENV PATH="/root/.poetry/bin:${PATH}"

# Poetry dependencies
COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN poetry config virtualenvs.create false

# Copy your application files to the container
COPY /python_loaders .