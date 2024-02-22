#dockerfile, image, docker container

FROM python:3.8-slim

ADD dol-main.py .

RUN pip install pandas pymongo sqlalchemy ijson

CMD ["python","dol-main.py"]


