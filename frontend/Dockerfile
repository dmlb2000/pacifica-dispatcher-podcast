FROM python:3.8

RUN mkdir /usr/src/app
WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /usr/src/app/
CMD python src/server.py