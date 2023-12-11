"""Notification module"""
from json import dumps
from pymongo import MongoClient
from bson.json_util import dumps
import pandas as pd
from datetime import datetime, timedelta
import pandas as pd
# import schedule
import time
import json
import mysql.connector
from sqlalchemy import create_engine
import threading
from concurrent.futures import ThreadPoolExecutor


import os
import requests
import uvicorn
import redis
# from typing import Union
# import mysql.connector

from fastapi import FastAPI
from pydantic import BaseModel
from queue import Queue
from multiprocessing import Manager
from shared_memory_dict import SharedMemoryDict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from src.config_parser import Config
from src.createclient import CreateClient
from src.notification_summarization import create_dataframe
from src.fetch_data import Mongo_Data, Sql_Data
from src.eventbased_notification import event_alerts
from src.event_consumer import NotificationConsumer
from src.hourly_notification import hourly_alerts
from src.shiftbased_notification import shiftbased_alerts



os.environ["SHARED_MEMORY_USE_LOCK"]="1"

event_smd = SharedMemoryDict(name='event', size=10000000)
hourly_smd = SharedMemoryDict(name='hourly', size=10000000)
shiftbased_smd = SharedMemoryDict(name='shiftbased', size=10000000)

config = Config.yamlconfig("config/config.yaml")[0]
dbconfig=config["db"]
mongoconfig=config["mongodb"]
apiconfig=config["apis"]
kafkahost=config["kafka"]
clientobj =CreateClient(config)
mongo_collection =clientobj.mongo_client()

def testcallbackFuture(future):
    if not future.running():
        print("===>",future)
    print("=======result====")
    print(future.result())
    print("=======callback future====",future.exception())

def startevent_notification():
    eventobj = NotificationConsumer(kafkahost)
    consumer = eventobj.connectConsumer() ##
    eventobj.runConsumer(apiconfig['postnotificationalerts'])

# def starthourly_notification_old():        
#     start_time = datetime.now().replace(minute=0, second=0)-timedelta(hours=1) # # should be changed
#     end_time = datetime.now().replace(minute=0, second=0)             
    
#     hourly_alerts.run(mongo_collection, start_time, end_time)
    
def starthourly_notification():   
    while True:
        current_time = datetime.now()
        # if current_time.second >= 5 and current_time.second <= 10:
        if current_time.minute >= 5 and current_time.minute <= 10:             
            start_time = datetime.now().replace(minute=0, second=0)-timedelta(hours=1) # # should be changed
            # start_time = datetime.now().replace(minute=0, second=0)-timedelta(days=5) # # should be changed
            end_time = datetime.now().replace(minute=0, second=0)       
    
            hourly_alerts.run(mongo_collection, start_time, end_time, apiconfig['postnotificationalerts'])
            
        time.sleep(300)
        # time.sleep(5)

def startshiftbased_notification():  
        # completed_dict = {}
        shiftbased_alerts.run_v2(mongo_collection,apiconfig['postnotificationalerts'])

class Notification:
    def __init__(self,):
        self.config = Config.yamlconfig("config/config.yaml")[0]
        self.dbconfig=self.config["db"]
        self.mongoconfig=self.config["mongodb"]
        self.apiconfig=self.config["apis"]
        self.kafkahost=self.config["kafka"]
        self.clientobj =CreateClient(self.config)
        self.mongo_collection =self.clientobj.mongo_client()
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        self.r = redis.Redis(connection_pool=pool)
        self.future_dict = {}
    
    # def startevent_notification(self,):
    #     eventobj = NotificationConsumer(self.kafkahost)
    #     eventobj.connectConsumer() ##
    #     # print("here")
    #     eventobj.runConsumer()
        
    
        
    # def starthourly_notification(self,):
    #     while True:
    #         current_time=datetime.now()
        
    #         start_time = datetime.now().replace(minute=0, second=0)-timedelta(hours=1) # # should be changed
    #         end_time = datetime.now().replace(minute=0, second=0) 
                
            
    #         hourly_alerts.run(self.mongo_collection, start_time, end_time)
        
    # def startshiftbased_notification(self,):                     
    #     shiftbased_alerts.run(self.mongo_collection,)
        
    def notify(self,):
        event_executor = ThreadPoolExecutor(max_workers=3)
        hourly_executor = ThreadPoolExecutor(max_workers=3)
        shiftbased_executor = ThreadPoolExecutor(max_workers=3)
        # event_future = self.executor.submit(self.runConsumer)
        # hourly_future = self.executor.submit(self.minio_thread)
        # shiftbased_future = self.executor.submit(self.mongo_thread)
        while True:
            notification_data=json.loads(self.r.get("notifications"))
            event_dict = notification_data['event']
            hourly_dict = notification_data['hourly']
            shiftbased_dict = notification_data['shiftbased']
            
            for cam_id in event_dict:
                event_smd[cam_id]=event_dict[cam_id]
                
            for cam_id in hourly_dict:
                hourly_smd[cam_id]=hourly_dict[cam_id]
                
            for cam_id in shiftbased_dict:
                shiftbased_smd[cam_id]=shiftbased_dict[cam_id]
            # print("#"*100)
            # print(hourly_smd)
            # print("#"*100)
            # print(self.future_dict)    
            if "event" in self.future_dict:
                if not self.future_dict["event"].running():
                    self.future_dict['event']=event_executor.submit(startevent_notification)
                    print("===========callback====",self.future_dict['event'])
                    self.future_dict['event'].add_done_callback(testcallbackFuture)
                
            else: 
                self.future_dict['event']=event_executor.submit(startevent_notification) 
                print("===========callback====",self.future_dict['event'])
                self.future_dict['event'].add_done_callback(testcallbackFuture)
                    
            if "hourly" in self.future_dict:
                if not self.future_dict["hourly"].running():
                    self.future_dict['hourly']=hourly_executor.submit(starthourly_notification)
                    print("===========callback====",self.future_dict["hourly"])
                    self.future_dict["hourly"].add_done_callback(testcallbackFuture)
                
            else: 
                self.future_dict['hourly']=hourly_executor.submit(starthourly_notification) 
                print("===========callback====",self.future_dict["hourly"])
                self.future_dict["hourly"].add_done_callback(testcallbackFuture)
                
            if "shiftbased" in self.future_dict:
                if not self.future_dict["shiftbased"].running():
                    self.future_dict['shiftbased']=shiftbased_executor.submit(startshiftbased_notification)
                    print("===========callback====",self.future_dict["shiftbased"])
                    self.future_dict["shiftbased"].add_done_callback(testcallbackFuture)
                
            else: 
                self.future_dict['shiftbased']=shiftbased_executor.submit(startshiftbased_notification) 
                print("===========callback====",self.future_dict["shiftbased"])
                self.future_dict["shiftbased"].add_done_callback(testcallbackFuture)

                    
                    
                
                
        
    # def get_data():
    #     # while