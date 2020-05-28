FROM python:3.8

RUN mkdir /usr/src/app
WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY *.py /usr/src/app/
CMD celery -A src.tasks worker --loglevel=info
