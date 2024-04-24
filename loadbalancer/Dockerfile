FROM python:latest

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip install --upgrade pip
RUN apt-get install -y default-mysql-server

COPY ./loadbalancer ./loadbalancer
COPY ./utils /utils
COPY ./requirements.txt /requirements.txt

RUN pip install -r requirements.txt

EXPOSE 5000

ENTRYPOINT  [ "bash" ]
CMD [ "./loadbalancer/deploy.sh"]

# run using:
# docker run --rm -p 5000:5000 --name loadbalancerim dloadb:latest