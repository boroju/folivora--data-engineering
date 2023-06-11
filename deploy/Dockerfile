# Dockerfile

# Use the base Python 3.9 image
FROM python:3.9

RUN apt-get update \
    &&  apt-get install -y \
        build-essential \
        cmake \
        libsasl2-dev \
        curl

# Set the working directory to /python_loaders
WORKDIR /python_loaders

RUN pip install -U pip

# Poetry installation
RUN pip install poetry==1.1.15
ENV PATH="/root/.poetry/bin:${PATH}"

# Poetry dependencies
COPY pyproject.toml  /python_loaders/pyproject.toml
COPY poetry.lock  /python_loaders/poetry.lock
#Hack to avoid distutils error TODO: Try removing manually from 'site-packages'
#RUN pip install entrypoints==0.3 --ignore-installed

RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction -v

# Copy your application files to the container
COPY . /python_loaders