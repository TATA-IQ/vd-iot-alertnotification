from multiprocessing import Manager
from datetime import datetime, timedelta
import time
from shared_memory_dict import SharedMemoryDict
import os
import requests
import json
import logging
import multiprocessing as mp
os.environ["SHARED_MEMORY_USE_LOCK"]="1"

from src.config_parser import Config
from src.createclient import CreateClient
from src.notification_summarization import create_dataframe
from src.fetch_data import Mongo_Data, Sql_Data
from src.eventbased_notification import event_alerts
from src.hourly_notification import hourly_alerts

shiftbased_smd = SharedMemoryDict(name='shiftbased', size=10000000)

class shiftbased_alerts:
    def get_shift(shiftbased_smd):        
        nps=[]
        for i in shiftbased_smd:
            for j in shiftbased_smd[i]:
                for k in shiftbased_smd[i][j]:
                    nps.append(k)
        return nps
    
    def get_shift_time(nps):
        np_inter={}
        for i in nps:
            end_time = i["end_time"]
            if end_time not in np_inter:        
                np_inter[end_time]=[]
            np_inter[end_time].append(i)
        return np_inter
    
    def create_notification(np, s, start_time, end_time):
        for w in np:
            df_not = s[(s['camera_id']==np["camera_id"]) & (s['usecase_id']==np["usecase_id"])]
            not_list = []
            tot_list = []
            if(len(df_not)>0):
                par = []
                cnt = 0
                for i in range(0,len(df_not)):
                    par.append({'usecase_id': df_not.iloc[i]['usecase_id'], 'usecase_name':df_not.iloc[i]['usecase_name'], 'incident_id':df_not.iloc[i]['incident_id'], 'incident_id':df_not.iloc[i]['incident_name'], 'camera_id':df_not.iloc[i]['camera_id'], 'camera_id':df_not.iloc[i]['camera_name'], 'incident_count': df_not.iloc[i]['incident_count']})
                    cnt=cnt+df_not.iloc[i]['incidnt_count']
                not_list.append(w['notification_id'])
                tot_list.append(cnt)
                d = {'notification_id': not_list, 'count':tot_list, 'params':par, 'start_time': start_time, 'end_time':end_time}
        return d
    
    
    # def run(mongo_collection):       
        
    #     nps = shiftbased_alerts.get_shift(shiftbased_smd)
    #     for np in nps:
    #         # print("here") 
    #         today_timestampdate = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
    #         camera_id = np['camera_id']            
    #         usecase_id = np['usecase_id']    
    #         start_time = np['start_time']           
    #         end_time = np['end_time']   
    #         start_time =(today_timestampdate+start_time)   
    #         end_time =(today_timestampdate+end_time)
    #         print("start,end",start_time, end_time)
                
    #         list_cur = Mongo_Data.get_shiftbaseddata(mongo_collection, camera_id, usecase_id, start_time, end_time)
    #         print(len(list_cur))
                
    #         print("start_time, end_time ",start_time, end_time)
    #         if len(list_cur)>0:
    #             dataframe_obj = create_dataframe()
    #             df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
    #             df_final = dataframe_obj.summarization(df_all)
    #             # np=hourly_smd # # read from database
                
    #             d =  hourly_alerts.create_notification(np, df_final,  start_time,end_time)
    #             url = "http://172.16.0.170:7575/api/v1/mailservice/email/notification"
    #             r = requests.post(url, json=json.dumps(d))


                    
    #         return 
    
    def run(mongo_collection, url):  
             
        # if shiftbased_smdold != shiftbased_smd:
        #     np_inter.update
        #         if np_inter.keys() in completed_np_inter:
                    
        completed_np_inter={} # datewise key
        current_day_completed_end_times = []
        
        while True:
            nps = shiftbased_alerts.get_shift(shiftbased_smd)
            np_inter = shiftbased_alerts.get_shift_time(nps)
            current_time = time.time()
            print("current_time==",current_time)
            for each_end_time in np_inter:
                print(f"for each_end_time: {each_end_time}")
                end_timestamp = (datetime.now().replace(hour=0,minute=0, second=0, microsecond=0)+timedelta(seconds=each_end_time)).timestamp()
                print("condition",current_time, end_timestamp+300)
                if current_time >= end_timestamp + 300:
                    print("in here")
                    completed_np_inter[each_end_time]=np_inter[each_end_time]
                    np_final = {}
                    for i in np_inter[each_end_time]:
                        notification_id = i["notification_id"]
                        if notification_id not in np_final:        
                            np_final[notification_id]=[]
                        np_final[notification_id].append(i)
                    # print("*"*100)
                    # print(np_final)
                    for not_id in np_final:
                        res = {"notification_id":int(not_id),"total_count":0,"params":[]}
                        for np in np_final[not_id]:               
                            today_timestampdate = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
                            camera_id = np['camera_id']            
                            usecase_id = np['usecase_id']    
                            start_time = np['start_time']           
                            end_time = np['end_time'] 
                            start_time = 1698230545000   
                            # start_time =(today_timestampdate+start_time)   
                            end_time =int(today_timestampdate+end_time)*1000
                            print("start,end",start_time, end_time)
                                
                            list_cur = Mongo_Data.get_shiftbaseddata(mongo_collection, camera_id, usecase_id, start_time, end_time)
                            print(len(list_cur))
                                
                            print("start_time, end_time ",start_time, end_time)
                            if len(list_cur)>0:
                                dataframe_obj = create_dataframe()
                                df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
                                df_final = dataframe_obj.summarization(df_all)
                            # np=hourly_smd # # read from database
                            
                                d = hourly_alerts.create_notification(np, df_final, start_time,end_time)
                                # print(f" for notification id {not_id} d: {d}")
                                res['total_count'] += d["total_count"][0]
                                res['params'].append(d["params"][0])
                            
                        print("res dict===", res)
                        # r = requests.post(url, json=json.dumps(res))
                        # print(f"Status Code: {r.status_code}, Response: {r.json()}")
                        
                    # nps.remove(np)
                    del np_inter[each_end_time]
                    
                    
            time.sleep(60)
            
    def run_v2(mongo_collection, url):  
                    
        completed_np_inter={} # datewise key
        current_day_completed_end_times = {}
        print("current_day_completed_end_times===",current_day_completed_end_times)
        while True:
            nps = shiftbased_alerts.get_shift(shiftbased_smd)
            np_inter = shiftbased_alerts.get_shift_time(nps)
            current_time = time.time()
            print("current_time==",current_time)
            for each_end_time in np_inter:
                print(f"for each_end_time: {each_end_time}")
                end_timestamp = (datetime.now().replace(hour=0,minute=0, second=0, microsecond=0)+timedelta(seconds=each_end_time)).timestamp()
                print("condition",current_time, end_timestamp+300)
                if current_time >= end_timestamp + 300:
                    print("in here")
                    date_key = datetime.now().date()#.strftime("%Y-%m-%d")
                    if date_key not in current_day_completed_end_times:
                        current_day_completed_end_times[date_key] = []
                        
                    if each_end_time not in current_day_completed_end_times[date_key]:
                        current_day_completed_end_times[date_key].append(each_end_time)
                        completed_np_inter[each_end_time] = np_inter[each_end_time]
                        
                        np_final = {}                    
                        for i in np_inter[each_end_time]:
                            notification_id = i["notification_id"]
                            if notification_id not in np_final:        
                                np_final[notification_id]=[]
                            np_final[notification_id].append(i)
                        # print("*"*100)
                        # print(np_final)
                        for not_id in np_final:
                            res = {"notification_id":int(not_id),"total_count":0,"params":[]}
                            for np in np_final[not_id]:               
                                today_timestampdate = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
                                camera_id = np['camera_id']            
                                usecase_id = np['usecase_id']    
                                start_time = np['start_time']           
                                end_time = np['end_time'] 
                                start_time = 1698230545000   
                                # start_time =(today_timestampdate+start_time)   
                                end_time =int(today_timestampdate+end_time)*1000
                                print("start,end",start_time, end_time)
                                    
                                list_cur = Mongo_Data.get_shiftbaseddata(mongo_collection, camera_id, usecase_id, start_time, end_time)
                                print(len(list_cur))
                                    
                                print("start_time, end_time ",start_time, end_time)
                                if len(list_cur)>0:
                                    dataframe_obj = create_dataframe()
                                    df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
                                    df_final = dataframe_obj.summarization(df_all)
                                # np=hourly_smd # # read from database
                                
                                    d = hourly_alerts.create_notification(np, df_final, start_time,end_time)
                                    # print(f" for notification id {not_id} d: {d}")
                                    res['total_count'] += d["total_count"][0]
                                    res['params'].append(d["params"][0])
                            print(current_day_completed_end_times)
                            print(f"each end time: {each_end_time} and notification_id : {not_id}")    
                            print("res dict===", res)
                            # r = requests.post(url, json=json.dumps(res))
                            # print(f"Status Code: {r.status_code}, Response: {r.json()}")
                            
                        # nps.remove(np)
                        # del np_inter[each_end_time]
                        # current_day_completed_end_times[date_key].append(each_end_time)
                        if len(current_day_completed_end_times)>2:
                            del current_day_completed_end_times[sorted(current_day_completed_end_times)[0]]
                    
                    
            time.sleep(60)
    # def run_working(mongo_collection, url):       
        
    #     nps = shiftbased_alerts.get_shift(shiftbased_smd)
    #     completed_nps=[]
    #     while True:
    #         current_time = time.time()
    #         for np in nps:
    #             end_time = np['end_time'] 
    #             if current_time >= end_time + 300:
    #                 completed_nps.append(np)
    #                 today_timestampdate = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
    #                 camera_id = np['camera_id']            
    #                 usecase_id = np['usecase_id']    
    #                 start_time = np['start_time']           
    #                 end_time = np['end_time']   
    #                 start_time =(today_timestampdate+start_time)   
    #                 end_time =(today_timestampdate+end_time)
    #                 print("start,end",start_time, end_time)
                        
    #                 list_cur = Mongo_Data.get_shiftbaseddata(mongo_collection, camera_id, usecase_id, start_time, end_time)
    #                 print(len(list_cur))
                        
    #                 print("start_time, end_time ",start_time, end_time)
    #                 if len(list_cur)>0:
    #                     dataframe_obj = create_dataframe()
    #                     df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
    #                     df_final = dataframe_obj.summarization(df_all)
    #                     # np=hourly_smd # # read from database
                        
    #                     d =  hourly_alerts.create_notification(np, df_final, start_time,end_time)
    #                     r = requests.post(url, json=json.dumps(d))
    #                     print(f"Status Code: {r.status_code}, Response: {r.json()}")
                    
    #                 nps.remove(np)
                    
                    
    #         time.sleep(60)
            
            
            
            
            
        # for np in nps:
        #     # print("here") 
                
                
        #     today_timestampdate = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
        #     camera_id = np['camera_id']            
        #     usecase_id = np['usecase_id']    
        #     start_time = np['start_time']           
        #     end_time = np['end_time']   
        #     start_time =(today_timestampdate+start_time)   
        #     end_time =(today_timestampdate+end_time)
        #     print("start,end",start_time, end_time)
                
        #     list_cur = Mongo_Data.get_shiftbaseddata(mongo_collection, camera_id, usecase_id, start_time, end_time)
        #     print(len(list_cur))
                
        #     print("start_time, end_time ",start_time, end_time)
        #     if len(list_cur)>0:
        #         dataframe_obj = create_dataframe()
        #         df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
        #         df_final = dataframe_obj.summarization(df_all)
        #         # np=hourly_smd # # read from database
                
        #         d =  hourly_alerts.create_notification(np, df_final,  start_time,end_time)
        #         url = "http://172.16.0.170:7575/api/v1/mailservice/email/notification"
        #         r = requests.post(url, json=json.dumps(d))


                    
        #     return 