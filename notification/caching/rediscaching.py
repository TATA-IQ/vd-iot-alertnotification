import redis
import requests
import json
import threading
from kafka import KafkaConsumer
from caching.notification_caching import PersistTopic


class Caching:
    """
    This class handles the caching of the respective module
    And always listens for the event changes in kafka.
    If any event is encountered it will update the caching.
    """
    def __init__(
        self,
        api: dict,  
    ) -> None:  # noqa: E501
        """
        Initialize the caching
        Args:
            api: dict of apis
        """
        pool = redis.ConnectionPool(host="localhost", port=6379, db=0)
        self.r = redis.Redis(connection_pool=pool)
        self.api = api
        # self.topic = PersistTopic()
        # self.topic = "incident_event"
        
        
        # self.urllist=urllist
    def get_notification_config(self,camera_id=None):
        print(self.api["getnotificationdetails"])
        r = requests.get(self.api["getnotificationdetails"],json={"camera_id":camera_id})
        try:
            return r.json()["data"]
        except Exception as ex:
            print("notification exception: ", ex)
            return {}
            
    def get_notification_typedata(self,data):
        """ 
        converts data in list of respective notifciation type dicts
        """
        event = {}
        hourly = {}
        shiftbased = {}
        for i in data:
            if i['frequency_id']==3:
                if i['camera_id'] not in event:
                    event[i['camera_id']]={}
                    if i['usecase_id'] not in event[i['camera_id']]:
                        event[i['camera_id']][i['usecase_id']]=[i]
                    else:
                        event[i['camera_id']][i['usecase_id']].append(i)
                else:
                    if i['usecase_id'] not in event[i['camera_id']]:
                        event[i['camera_id']][i['usecase_id']]=[i]
                    else:
                        event[i['camera_id']][i['usecase_id']].append(i)

            elif i['frequency_id']==1:
                if i['camera_id'] not in hourly:
                    hourly[i['camera_id']]={}
                    if i['usecase_id'] not in hourly[i['camera_id']]:
                        hourly[i['camera_id']][i['usecase_id']]=[i]
                    else:
                        hourly[i['camera_id']][i['usecase_id']].append(i)
                else:
                    if i['usecase_id'] not in hourly[i['camera_id']]:
                        hourly[i['camera_id']][i['usecase_id']]=[i]
                    else:
                        hourly[i['camera_id']][i['usecase_id']].append(i)

            elif i['frequency_id']==2:
                if i['camera_id'] not in shiftbased:
                    shiftbased[i['camera_id']]={}
                    if i['usecase_id'] not in shiftbased[i['camera_id']]:
                        shiftbased[i['camera_id']][i['usecase_id']]=[i]
                    else:
                        shiftbased[i['camera_id']][i['usecase_id']].append(i)
                else:
                    if i['usecase_id'] not in shiftbased[i['camera_id']]:
                        shiftbased[i['camera_id']][i['usecase_id']]=[i]
                    else:
                        shiftbased[i['camera_id']][i['usecase_id']].append(i)
                        
        return event, hourly, shiftbased

    def persistData(self):
        dictcache={}
        notification_conf = self.get_notification_config()
        print("=======notification confi================")
        print(notification_conf)
        print("*"*100)
        # print("=====notification_conf",notification_conf)
        eventdict, hourlydict, shiftbaseddict = self.get_notification_typedata(notification_conf)
        

        dictcache["event"]=eventdict
        dictcache["hourly"]=hourlydict
        dictcache["shiftbased"]=shiftbaseddict
        # topicconfig={}
        # jsonreq = {
            
        #     "notication_details": notification_conf
        # }
        # topicconfig=self.topic.persistData(jsonreq)
        
        print("dictcache==== ",dictcache)
        self.r.set("notifications", json.dumps(dictcache))

    def checkEvents(self):
        consumer = KafkaConsumer(
            "dbevents",
            bootstrap_servers=[
                "172.16.0.175:9092",
                "172.16.0.171:9092",
                "172.16.0.174:9092",
            ],
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        for message in consumer:
            if message is None:
                continue
            else:
                self.persistData()



