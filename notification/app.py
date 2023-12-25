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
import consul
import requests
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
from console_logging.console import Console
console=Console()

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
def get_service_address(consul_client,service_name,env):
    while True:
        
        try:
            services=consul_client.catalog.service(service_name)[1]
            print(services)
            for i in services:
                if env == i["ServiceID"].split("-")[-1]:
                    return i
        except:
            time.sleep(10)
            continue



def get_confdata(consul_conf):
    consul_client = consul.Consul(host=consul_conf["host"],port=consul_conf["port"])
    pipelineconf=get_service_address(consul_client,"pipelineconfig",consul_conf["env"])

    alertconf=None
    dbconf=None
    
    env=consul_conf["env"]
    
    endpoint_addr="http://"+pipelineconf["ServiceAddress"]+":"+str(pipelineconf["ServicePort"])
    print("endpoint addr====",endpoint_addr)
    while True:
        
        try:
            res=requests.get(endpoint_addr+"/")
            endpoints=res.json()
            print("===got endpoints===",endpoints)
            break
        except Exception as ex:
            print("endpoint exception==>",ex)
            time.sleep(10)
            continue
    
    while True:
        try:
            res=requests.get(endpoint_addr+endpoints["endpoint"]["alert"])
            alertconf=res.json()
            print("alertconf===>",alertconf)
            break
            

        except Exception as ex:
            print("containerconf exception==>",ex)
            time.sleep(10)
            continue
    while True:
        try:
            res=requests.get(endpoint_addr+endpoints["endpoint"]["kafka"])
            kafkaconf=res.json()
            print("kafkaconf===>",kafkaconf)
            break
            

        except Exception as ex:
            print("containerconf exception==>",ex)
            time.sleep(10)
            continue
    print("=======searching for dbapi====")
    while True:
        try:
            print("=====consul search====")
            dbconf=get_service_address(consul_client,"dbapi",consul_conf["env"])
            print("****",dbconf)
            dbhost=dbconf["ServiceAddress"]
            dbport=dbconf["ServicePort"]
            res=requests.get(endpoint_addr+endpoints["endpoint"]["dbapi"])
            dbres=res.json()
            print("===got db conf===")
            print(dbres)
            break
        except Exception as ex:
            print("db discovery exception===",ex)
            time.sleep(10)
            continue
    for i in dbres["apis"]:
        print("====>",i)
        dbres["apis"][i]="http://"+dbhost+":"+str(dbport)+dbres["apis"][i]

    
    print("======dbres======")
    print(dbres)
    print(alertconf)
    return  dbres,alertconf,kafkaconf

if __name__ == "__main__":
    
    os.makedirs("logs", exist_ok=True)
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
        
        config = Config.yamlconfig("config/config.yaml")[0]
        dbres,alertconf,kafkaconf=get_confdata(config["consul"])
        mongoconfig=alertconf["mongodb"]
        dbconfig=alertconf["db"]
        notification_api=alertconf["notification_api"]
        notobj = Notification(alertconf,dbconfig,mongoconfig,kafkaconf,dbres["apis"],notification_api,logger)
        notobj.notify()
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
    

# schedule.every().hour.at(":05").do(run)