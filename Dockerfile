FROM python:3.9-slim-buster
RUN apt-get update
RUN apt-get install -y redis-server
RUN pip install pandas
RUN pip install mysql-connector-python
RUN pip install SQLAlchemy  
RUN pip install pymongo
RUN pip install kafka-python
RUN pip install redis
RUN pip install requests
RUN pip install protobuf==3.20.*
RUN pip install shared-memory-dict
RUN pip install PyYaml
Run pip install opencv-python
Run pip install schedule
Run pip install 'fastapi[all]'
copy notification/ /app
WORKDIR /app
# RUN mkdir /app/logs
CMD chmod +x run.sh
CMD ./run.sh