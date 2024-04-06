FROM jupyter/pyspark-notebook:python-3.11.6
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /spark
CMD [ "python", "./consumer.py" ]