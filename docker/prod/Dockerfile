FROM python:3.11.3

# install library
RUN apt-get update \
    && apt-get install -y git

# TimeZone
RUN apt-get update \
    && apt install -y tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV TZ Asia/Tokyo

# copy apps
COPY ./ /working

# pip install
WORKDIR /working
RUN pip install -U pip && \
    pip install -r ./docker/requirements.txt

# python path
ENV PYTHONPATH /working

CMD [""]
