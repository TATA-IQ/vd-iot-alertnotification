from multiprocessing import Manager
from shared_memory_dict import SharedMemoryDict
import os
import requests
import json
import multiprocessing as mp
from console_logging.console import Console
console=Console()
os.environ["SHARED_MEMORY_USE_LOCK"]="1"

event_smd = SharedMemoryDict(name='event', size=10000000)

class event_alerts:
    # # def create_notification(np,data):
    #     result_dict={}
    #     not_list = []
    #     tot_list = []
    #     for camid in event_smd:
    #         print(event_smd[camid])
    #         for usecaseid in event_smd[camid]:
    #             for np in event_smd[camid][usecaseid]:
    #                 if(data['usecase']['id'] == np['usecase_id'] and data['hierarchy']['camera_id'] == np['camera_id']):
    #                     print('yes')
    #                     par = []
    #                     cnt = 0
    #                     for inc in data['incident']:
    #                         if(inc['incident_id']==np['incident_id']):
    #                             par.append({'usecase_id': data['usecase']['id'], 'usecase_name':data['usecase']['name'], 'incident_id':inc['incident_id'], 'incident_name': inc['name'], 'camera_id': data['hierarchy']['camera_id'], 'camera_name': data['hierarchy']['camera_name'],'incident_count':1 })
    #                             cnt+=1
    #                     not_list.append(np['notification_id'])
    #                     tot_list.append(cnt)
    #                     d = {'notification_id': not_list, 'total_count':tot_list, 'params':par}
    #     return d
                                        
    #         # for np in event_smd[camid]:
    #         #     print(np)
    #         # for np in np_main:
    #         #     print
    #         #     print(event_smd[np_main][np])
    
    def create_notification(np,data):
        not_list = []
        tot_list = []
        if(data['usecase']['usecase_id'] == np['usecase_id'] and data['hierarchy']['camera_id'] == np['camera_id']):
            print('yes')
            par = []
            cnt = 0
            for inc in data['usecase']['incident']:
                if(inc['incident_id']==np['incident_id']):
                    par.append({'usecase_id': data['usecase']['usecase_id'], 'usecase_name':data['usecase']['name'], 'incident_id':inc['incident_id'], 'incident_name': inc['name'], 'camera_id': data['hierarchy']['camera_id'], 'camera_name': data['hierarchy']['camera_name'],'incident_count':1 })
                    cnt+=1
            not_list.append(np['notification_id'])
            tot_list.append(cnt)
            d = {'notification_id': not_list, 'total_count':tot_list, 'params':par}
            # r = requests.post(url, json=json.dumps(d))
            # print(f"Status Code: {r.status_code}, Response: {r.json()}")
        return d
    
    