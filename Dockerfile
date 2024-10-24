FROM python:3.8-slim

ADD https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem .
ADD main.py helpers.py requirements.txt .env  .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python","./main.py"]

