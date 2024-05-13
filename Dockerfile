FROM python:latest

RUN apt-get update
RUN apt-get install curl -y
RUN apt-get install python3 python3-pip -y
RUN pip3 install --upgrade pip
WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip3 install -r requirements.txt

COPY ./app/main.py /app
COPY ./app/source-data /app/source-data

# Uncomment the following line to enable debugging in the container
# CMD python3 -m debugpy --wait-for-client --listen 0.0.0.0:5678 main.py

CMD python3 main.py
