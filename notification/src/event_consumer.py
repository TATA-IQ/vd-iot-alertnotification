from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import json
import requests 
import base64
import threading
import redis
import time
from datetime import datetime
import os
import ast
from queue import Queue
from kafka import TopicPartition
from shared_memory_dict import SharedMemoryDict

from console_logging.console import Console
console=Console()

from src.eventbased_notification import event_alerts
os.environ["SHARED_MEMORY_USE_LOCK"]="1"

event_smd = SharedMemoryDict(name='event', size=10000000)
class NotificationConsumer():
    def __init__(self,kafkashost, logger):
        self.kill=False
        self.kafkahost=kafkashost
        self.logger = logger
        self.logger.info("*******kafkahost*****{self.kafkahost}")
        console.info("*******kafkahost*****{self.kafkahost}")
        self.consumer=None
        # self.log=logger
        self.check=False
        self.previous_time=datetime.now()
        self.topic="incident_event"
        
        # self.log.info(f"Starting for {self.camera_id} and topic {self.topic}")
        
    def closeConsumer(self):
        if self.consumer:
            self.consumer.close()
            return True
        else:
            return False
    
    def connectConsumer(self):
        
        # self.queue=Queue(100)
        self.consumer=KafkaConsumer(self.topic, bootstrap_servers=self.kafkahost, auto_offset_reset="latest",
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')),group_id=self.topic)
        # self.log.info(f"Connected Consumer {self.camera_id} for {self.topic}")
        

        return self.consumer
        
    def isConnected(self):
        #print("====Check Self COnsumer====",self.consumer)
        return self.consumer.bootstrap_connected()
    def messageParser(self,msg):
        #msg=ast.literal_eval(msg)
        try:
            msg=json.loads(msg.value)
        except Exception as e:
            self.logger.info(f"message {msg}, exp {e}")
        # print(msg)
            
        incident_event = msg['incident_event']
        return incident_event
        #your message parser code
        
    def get_event_nps(self,event_nps):
        self.logger.info("in get event nps")
        np_final={}
        for i in event_nps:
            notification_id = i["notification_id"]
            if notification_id not in np_final:        
                np_final[notification_id]=[]
            np_final[notification_id].append(i)
        return np_final
    
    def runConsumer(self, url):
        self.logger.info(self.consumer)
        self.check=True        
        self.logger.info("here in run consumer")
        console.info("here in run consumer")
        # print(consumer)
        # while True:
        #     print(self.consumer)
        for message in self.consumer:
        # for message in consumer:

            self.logger.info("===consumer running for a message=====")
            boolevent=False
            data = self.messageParser(message)
            camera_id = data['hierarchy']['camera_id']
            usecase_id = data['usecase']['usecase_id']
            self.logger.info(f"camera_id: {camera_id} and usecase__id: {usecase_id}")
            # print("*"*100)
            # # print(event_smd)
            # print("*"*100)
            # print(data)
            
            try:
                if event_smd[str(camera_id)][str(usecase_id)]:
                    self.logger.info(f"in event smd for cam id {camera_id} and usecase_id {usecase_id} ")
                # if event_smd[camera_id][usecase_id]:
                    boolevent=True
                    # print("9"*100)
                    # print(event_smd[str(camera_id)][str(usecase_id)])
                    # print("9"*100)
                    event_nps=event_smd[str(camera_id)][str(usecase_id)]
                    # event_nps=event_smd[camera_id][usecase_id]
                    # print("5"*100)
                    # print(event_nps)
                    # print("5"*100)
                    np_final = NotificationConsumer.get_event_nps(event_nps)
                    # print("#"*100)
                    # print(np_final)
                    # print("#"*100)
                    for not_id in np_final:
                        self.logger.info("here in run consumer np final ")
                        res = {"notification_id":int(not_id),"total_count":0,"params":[]}
                        for np in np_final[not_id]:
                            d = event_alerts.create_notification(np, data)
                            
                            res['total_count'] += d["total_count"][0]
                            res['params'].append(d["params"][0])

                        self.logger.info(f"res=================>{res}")
                        console.info(f"res=================>{res}")
                        try:
                            r = requests.post(url, json=json.dumps(res))
                            self.logger.info(f"Status Code: {r.status_code}, Response: {r.json()}")
                            console.info(f"Status Code: {r.status_code}, Response: {r.json()}")
                        except Exception as e:
                            self.logger.error(f"exception raised in event consumer{e}")
                            console.info(f"exception raised in event consumer{e}")
                    # for np in event_smd[str(camera_id)][str(usecase_id)]:
                    #     event_alerts.create_notification(np, data, url)          
                
            except:
                boolevent=False
                print(f"couldn't find event for camera_id: {camera_id} and usecase_id: {usecase_id}")
                self.logger.error(f"couldn't find event for camera_id: {camera_id} and usecase_id: {usecase_id}")
                console.error(f"couldn't find event for camera_id: {camera_id} and usecase_id: {usecase_id}")
                
            
            
