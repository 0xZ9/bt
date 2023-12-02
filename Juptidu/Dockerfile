FROM python:3.11

ADD src /src
ADD requirements.txt /
ADD data /data
RUN mkdir -p /result

RUN pip install -r requirements.txt

CMD [ "python3", "-u","/src/main.py" ]
