FROM alpine:3

ADD requirements.txt /requirements.txt

RUN \
    apk add --update --no-cache python3 g++ python3-dev libffi-dev openssl-dev cmd:pip3 build-base tzdata gcc musl-dev cargo && \
    echo $TZ > /etc/timezone \
    pip3 install --upgrade pip && \
    pip3 install wheel setuptools && \
    pip3 install -r requirements.txt --ignore-installed six && \
    pip3 freeze > requirements.txt

WORKDIR /app

CMD ["python3", "main.py"]
