FROM python:3.11.3

# install library
RUN apt-get update \
    && apt-get install -y git 

# pip install
COPY requirements.txt .

RUN pip install -U pip && \
    pip install -r requirements.txt
