FROM python:3-alpine


COPY . /app

WORKDIR /app

RUN \
 apk add --no-cache postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
 python3 -m pip install -r requirements.txt --no-cache-dir && \
 apk --purge del .build-deps

RUN chmod 444 main.py
RUN chmod 444 requirements.txt

ENV PORT 8080

CMD ["python3", "main.py"]
