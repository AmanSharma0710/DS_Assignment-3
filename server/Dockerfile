FROM python:latest

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip install --upgrade pip
RUN apt-get install -y default-mysql-server

RUN mkdir /src
WORKDIR /src
COPY server.py /src/server.py
COPY deploy.sh /src/deploy.sh
COPY logger.py /src/logger.py
COPY requirements.txt /src/requirements.txt

RUN pip install -r requirements.txt

EXPOSE 5000

ENTRYPOINT  [ "bash" ]
CMD [ "deploy.sh"]

# run using:
# docker run --rm -p 5001:5000 --name server1 -e serverID=123 dserver:latest