from multiprocessing import Manager
from datetime import datetime, timedelta
from shared_memory_dict import SharedMemoryDict
import os
import requests
import json
import multiprocessing as mp
os.environ["SHARED_MEMORY_USE_LOCK"]="1"

from src.config_parser import Config
from src.createclient import CreateClient
from src.notification_summarization import create_dataframe
from src.fetch_data import Mongo_Data, Sql_Data
from src.eventbased_notification import event_alerts
from console_logging.console import Console
console=Console()

hourly_smd = SharedMemoryDict(name='hourly', size=10000000)


class hourly_alerts:
    def __init__(self, mongo_collection, start_time, end_time, notification_url, logger):
        print("in hourly alerts module")
        console.info("in hourly alerts module")
        self.mongo_collection=mongo_collection
        self.start_time = start_time
        self.end_time = end_time
        self.notification_url = notification_url
        self.logger = logger
    
    def get_hourly(hourly_smd):        
        nps=[]
        for i in hourly_smd:
            for j in hourly_smd[i]:
                for k in hourly_smd[i][j]:
                    nps.append(k)
        return nps

    def get_hourly_config(nps):
        np_final={}
        for i in nps:
            notification_id = i["notification_id"]
            if notification_id not in np_final:        
                np_final[notification_id]=[]
            np_final[notification_id].append(i)
        return np_final
    
    def create_notification(np, s, start_time, end_time):
        print("======np======== ",np )
        for w in [np]:
            df_not = s[(s['camera_id']==np["camera_id"]) & (s['usecase_id']==np["usecase_id"])]
            not_list = []
            tot_list = []
            if(len(df_not)>0):
                par = []
                cnt = 0
                for i in range(0,len(df_not)):
                    par.append({'usecase_id': df_not.iloc[i]['usecase_id'], 'usecase_name':df_not.iloc[i]['usecase_name'], 'incident_id':df_not.iloc[i]['incident_id'], 'incident_id':df_not.iloc[i]['incident_name'], 'camera_id':df_not.iloc[i]['camera_id'], 'camera_name':df_not.iloc[i]['camera_name'], 'incident_count': df_not.iloc[i]['incident_count']})
                    cnt=cnt+df_not.iloc[i]['incident_count']
                console.info("===========here===========")
                console.info(w)
                print("===w['notification_id']====",w['notification_id'])
                not_list.append(w['notification_id'])
                tot_list.append(cnt)
                d = {'notification_id': not_list, 'total_count':tot_list, 'params':par, 'start_time': start_time, 'end_time':end_time}
        return d
    
    
    def run_old(mongo_collection, start_time, end_time, url):
        
        nps = hourly_alerts.get_hourly(hourly_smd)
        for np in nps:
            camera_id = np['camera_id']            
            usecase_id = np['usecase_id']            
            # hour = datetime.now().hour
            # dbconfig=config["db"]
            # mongoconfig=config["mongodb"]
            # apiconfig=config["apis"]

            # clientobj = CreateClient(config)
            # mongo_collection = clientobj.mongo_client()
            # start_time, end_time = Sql_Data.get_data(apiconfig["getsummarytime"]) ### replace with incident_time
            print("start, end",start_time, end_time)
            # if end_time==None:
            #     print(end_time)
            #     latest_start_time = datetime.now()-timedelta(days=1,hours=1) ## should be replaced with lowest time in mongo
            #     latest_end_time = datetime.now().replace(minute=0, second=0)
            # else:
            #     latest_start_time = datetime.strptime(end_time,'%Y-%m-%d %H:%M:%S')
            #     print("here")
            #     latest_end_time =  datetime.now().replace(minute=0, second=0)
            latest_start_time = start_time.replace(microsecond=0)
            latest_end_time = end_time.replace(microsecond=0)
            print("===###===",latest_start_time, latest_end_time)  
            print("diff",(latest_start_time-latest_end_time).total_seconds())
            if (latest_start_time-latest_end_time).total_seconds() != 0: ##
                try:
                    latest_start_time_str = latest_start_time.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    latest_start_time_str = None
                latest_end_time_str = latest_end_time.strftime('%Y-%m-%d %H:%M:%S')
                
                list_cur = Mongo_Data.get_hourlydata(mongo_collection, camera_id, usecase_id, latest_start_time_str, latest_end_time_str)
                print(len(list_cur))
                    
                print("latest_start_time_str, latest_end_time_str ",latest_start_time_str, latest_end_time_str)
                if len(list_cur)>0:
                    dataframe_obj = create_dataframe()
                    df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
                    df_final = dataframe_obj.summarization(df_all)
                    print(df_final)
                    # np=hourly_smd # # read from database
                    
                    d = hourly_alerts.create_notification(np, df_final,  latest_start_time_str,latest_end_time_str)
                    r = requests.post(url, json=json.dumps(d))
                    print(f"Status Code: {r.status_code}, Response: {r.json()}")
                    
            # return df_final
            
    def run(self,):
        mongo_collection = self.mongo_collection
        start_time = self.start_time
        end_time = self.end_time
        url = self.notification_url
        
        nps = hourly_alerts.get_hourly(hourly_smd)
        np_final = hourly_alerts.get_hourly_config(nps)
        for not_id in np_final:
            res = {"notification_id":int(not_id),"total_count":0,"params":[]}
            for np in np_final[not_id]:
                camera_id = np['camera_id']            
                usecase_id = np['usecase_id']            
                # hour = datetime.now().hour
                # dbconfig=config["db"]
                # mongoconfig=config["mongodb"]
                # apiconfig=config["apis"]

                # clientobj = CreateClient(config)
                # mongo_collection = clientobj.mongo_client()
                # start_time, end_time = Sql_Data.get_data(apiconfig["getsummarytime"]) ### replace with incident_time
                self.logger.info(f"start: {start_time}, end: {end_time}" )
                console.info(f"start: {start_time}, end: {end_time}" )
                # if end_time==None:
                #     print(end_time)
                #     latest_start_time = datetime.now()-timedelta(days=1,hours=1) ## should be replaced with lowest time in mongo
                #     latest_end_time = datetime.now().replace(minute=0, second=0)
                # else:
                #     latest_start_time = datetime.strptime(end_time,'%Y-%m-%d %H:%M:%S')
                #     print("here")
                #     latest_end_time =  datetime.now().replace(minute=0, second=0)
                latest_start_time = start_time.replace(microsecond=0)
                latest_end_time = end_time.replace(microsecond=0)
                self.logger.info(f"===# in hourly not##==={latest_start_time}, {latest_end_time}")  
                console.info(f"===##in hourly not#==={latest_start_time}, {latest_end_time}")  
                self.logger.info(f"diff in hourly not {(latest_end_time-latest_start_time).total_seconds()}")
                console.info(f"diff {(latest_end_time-latest_start_time).total_seconds()}")
                print(f"diff {(latest_end_time-latest_start_time).total_seconds()}")
                print("diff", (latest_end_time-latest_start_time).total_seconds())
                if (latest_end_time-latest_start_time).total_seconds() != 0: ##
                    try:
                        latest_start_time_str = latest_start_time.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        latest_start_time_str = None
                    latest_end_time_str = latest_end_time.strftime('%Y-%m-%d %H:%M:%S')
                    
                    list_cur = Mongo_Data.get_hourlydata(mongo_collection, camera_id, usecase_id, latest_start_time_str, latest_end_time_str)
                    self.logger.info(len(list_cur))
                        
                    print("latest_start_time_str, latest_end_time_str ",latest_start_time_str, latest_end_time_str)
                    self.logger.info(f"latest_start_time_str: {latest_start_time_str}, latest_end_time_str: {latest_end_time_str} in hourly not" )
                    console.info(f"latest_start_time_str: {latest_start_time_str}, latest_end_time_str: {latest_end_time_str} in hourly not" )
                    if len(list_cur)>0:
                        dataframe_obj = create_dataframe()
                        df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
                        df_final = dataframe_obj.summarization(df_all)
                        print(df_final)
                        # np=hourly_smd # # read from database
                        
                        d = hourly_alerts.create_notification(np, df_final,  latest_start_time_str,latest_end_time_str)
                        self.logger.info(f"=======d==={d}")
                        console.info(f"=======d==={d}")
                        res['total_count'] += d["total_count"][0]
                        res['params'].append(d["params"][0])
            
            self.logger.info(f"in hourly,  res dict==={res}" )
            console.info(f"in hourly, res dict==={res}" )
            print("res[totatlcount]",res['total_count'])
            if res['total_count']>0:
                try:
                    # r = requests.post(url, json=json.dumps(res))
                    url = url['postnotificationalerts']
                    console.info(f"url:- {url}")
                    r = requests.request("POST", url, data=json.dumps(res), headers={'Content-Type': 'application/json'})
                    self.logger.info(f"Status Code: {r.text}, Response: {r.text}")
                    console.info(f"Status Code: {r.text}, Response: {r.text}")
                except Exception as e:
                    self.logger.error(f"exception raised  in hourly not, {e}")
                    console.error(f"exception raised in hourly not, {e}")
            else:
                self.logger.info("in hourly, totalcount is 0")
                console.info("in hourly, totalcount is 0")
            # r = requests.post(url, json=json.dumps(res))
            # print(f"Status Code: {r.status_code}, Response: {r.json()}")
                    
            # return df_final