FROM python:latest

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip install --upgrade pip
RUN apt-get install -y default-mysql-server
RUN apt-get install -y docker.io

COPY ./shardmanager ./shardmanager
COPY ./requirements.txt /requirements.txt

RUN pip install -r requirements.txt

EXPOSE 5001

ENTRYPOINT  [ "bash" ]
CMD [ "./shardmanager/deploy.sh"]

# run using:
# docker run --rm -p 5000:5000 --name loadbalancerim dloadb:latest