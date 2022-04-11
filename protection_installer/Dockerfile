FROM alpine:3

ADD requirements.txt /requirements.txt

RUN \
    apk add --update --no-cache python3 python3-dev cmd:pip3 build-base tzdata && \
    echo $TZ > /etc/timezone \
    pip3 install --upgrade pip && \
    pip3 install wheel && \
    pip3 install -r requirements.txt

WORKDIR /app

CMD ["python3", "main.py"]
