"""main module"""
from json import dumps
from pymongo import MongoClient
# from bson.json_util import dumps
import pandas as pd
from datetime import datetime, timedelta
import pandas as pd
# import schedule
import time
import redis
# import mysql.connector
# from sqlalchemy import create_engine
import threading
from concurrent.futures import ThreadPoolExecutor


import os
import requests
import uvicorn
from typing import Union
import mysql.connector

from fastapi import FastAPI
from pydantic import BaseModel
from queue import Queue
from src.notification import Notification
# import multiprocessing as mp
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from shared_memory_dict import SharedMemoryDict
os.environ["SHARED_MEMORY_USE_LOCK"] = "1"

from sourcelogs.logger import create_rotating_log
from src.config_parser import Config
from src.createclient import CreateClient
from src.notification_summarization import create_dataframe
from src.fetch_data import Mongo_Data, Sql_Data
from src.eventbased_notification import event_alerts
from src.hourly_notification import hourly_alerts

os.environ["SHARED_MEMORY_USE_LOCK"]="1"

event_smd = SharedMemoryDict(name='event', size=10000000)
hourly_smd = SharedMemoryDict(name='hourly', size=10000000)
shiftbased_smd = SharedMemoryDict(name='shiftbased', size=10000000)


# class Notification:
#     def __init__(self,config):
#         self.config = Config.yamlconfig("config/config.yaml")[0]
#         self.dbconfig=self.config["db"]
#         self.mongoconfig=self.config["mongodb"]
#         self.apiconfig=self.config["apis"]
#         self.clientobj =self.CreateClient(config)
#         self.mongo_collection =self.clientobj.mongo_client()
        
#     # def hourly_process(self,):
        
    
    
        

# def run(self,):
#     hour = datetime.now().hour
#     print(f"summarization started at {hour}th hour")
#     config = Config.yamlconfig("config/config.yaml")[0]
#     dbconfig=config["db"]
#     mongoconfig=config["mongodb"]
#     apiconfig=config["apis"]

#     clientobj = CreateClient(config)
#     mongo_collection = clientobj.mongo_client()
#     start_time, end_time = Sql_Data.get_data(apiconfig["getsummarytime"]) ### replace with incident_time
#     print(start_time, end_time)
#     if end_time==None:
#         print(end_time)
#         latest_start_time = datetime.now()-timedelta(days=1,hours=1) ## should be replaced with lowest time in mongo
#         latest_end_time = datetime.now().replace(minute=0, second=0)
#     else:
#         latest_start_time = datetime.strptime(end_time,'%Y-%m-%dT%H:%M:%S')
#         latest_end_time =  datetime.now().replace(minute=0, second=0)
        
#     print("==###===",latest_start_time, latest_end_time)  
#     print((latest_start_time.replace(minute=0, second=0)-latest_end_time).total_seconds()) 
#     if (latest_start_time-latest_end_time).total_seconds() != 0: ##
#         try:
#             latest_start_time_str = latest_start_time.strftime('%Y-%m-%d %H:%M:%S')
#         except:
#             latest_start_time_str = None
#         latest_end_time_str = latest_end_time.strftime('%Y-%m-%d %H:%M:%S')
        
#         list_cur = Mongo_Data.get_data(self.mongo_collection, latest_start_time_str, latest_end_time_str)
#         print(len(list_cur))
            
#         print("latest_start_time_str, latest_end_time_str ",latest_start_time_str, latest_end_time_str)
#         if len(list_cur)>0:
#             dataframe_obj = create_dataframe()
#             df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
#             df_final = dataframe_obj.summarization(df_all)
#             np=None ## read from database
#             hourly_not_data =  hourly_alerts.create_notification(np,df_final,latest_start_time_str,latest_end_time_str)

# def shiftbased():
#     for w in np:
#         start_time = datepart + w['start_time']
#         end_time = datepart + w['end_time']
#         list_cur = Mongo_Data.get_data(mongo_collection, start_time, end_time)
#         print(len(list_cur))
            
#         print("latest_start_time_str, latest_end_time_str ",start_time, end_time)
#         if len(list_cur)>0:
#             dataframe_obj = create_dataframe()
#             df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
#             df_final = dataframe_obj.summarization(df_all)
#             np=None ## read from database
#             hourly_not_data =  hourly_alerts.create_notification(w,df_final,start_time,end_time)
        
        
# ## thread pool executor
    
# def run_thread():
#     hour_thread = threading.Thread(target = run)
#     hour_thread.start()
#     hour_thread.join()

# def schedule_summarization():
#     print("summarization started for hour", datetime.now().hour)
#     schedule.every().hour.do(run)
   
# def main():
#     executor = ThreadPoolExecutor(max_workers=3)
#     # schedule.every().hour.do(executor.submit, run) 
#     schedule.every(10).seconds.do(executor.submit, run)

#     while True:
#         schedule.run_pending()
#         time.sleep(1)

if __name__ == "__main__":
    logg = create_rotating_log("logs/logs.log")
    logger = create_rotating_log("logs/log.log")
    try:
        event_smd.shm.close()
        event_smd.shm.unlink()
        del event_smd
        hourly_smd.shm.close()
        hourly_smd.shm.unlink()
        del hourly_smd
        shiftbased_smd.shm.close()
        shiftbased_smd.shm.unlink()
        del shiftbased_smd
    except KeyboardInterrupt:
        print("=====Removing Shared Memory Refrence=====")
        event_smd.shm.close()
        event_smd.shm.unlink()     
        del event_smd
        hourly_smd.shm.close()
        hourly_smd.shm.unlink()     
        del hourly_smd
        shiftbased_smd.shm.close()
        shiftbased_smd.shm.unlink()     
        del shiftbased_smd
    # main()
    notobj = Notification()
    notobj.notify()

# schedule.every().hour.at(":05").do(run)