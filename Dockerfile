FROM python:3.9

WORKDIR /app

COPY requirements.txt .
COPY . /app

RUN apt-get update && apt-get install -y python3-dev libpq-dev libpq5 vim
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "main.py"]