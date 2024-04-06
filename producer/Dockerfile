FROM python:3.7.13-slim
WORKDIR /producer
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "./producer.py" ]
