# Introduction 
This is a alertnotification repo. 

# How It Works
1. Aggregate the notification group and get all the notification config and cache the data
2. Read notification configuration data from cache
3. Start processing for notification.
4. Check for the update in cache and update the notification configurations

# Architecture
![Architectural Flow](postprocessing/images/postprocess.png)

1. Notification API is hosted using uvicorn
2. To make the alertnotification fast, we are running multiple thread pool to process the request faster
3. Each threadpool are dedicated to handle different types of alerts such as event-based alerts, Shift-based alerts, and hourly alerts for notification.
4. Event-based Alerts are triggered by Kafka consumer events, Hourly Alerts are triggered each hour and Shift-based are triggered at the end of their respective shifts.
5. After processing the data, notifications are send as output.

# Dependency
1. This Module is dependent on the https://tatacommiot@dev.azure.com/tatacommiot/Video%20Based%20IoT/_git/vd-iot-dataapiservice
2. This module also needs kafka broker
3. This module will be dependent on post process service

# Installation
1. Install Python3.9 
2. Install redis-server(sudo apt-get install redis)
3. poetry install

# Run App
1. chmod +x run.sh
2. ./run.sh

# Docker 
1. Contenirization is enabled
2. change the config.yaml
2. Navigate to the Dockerfile level
2. build the container (sudo docker build -t "notification")
3. Run the container (sudo oocker run -t "notification")