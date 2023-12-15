import requests
from datetime import datetime
class Mongo_Data:
    def get_data(mongo_collection,start_time=None, end_time=None):
        print("in mongo data class")
        if start_time and end_time:
            print({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            start_time = int(str(int(start_time.timestamp()))+"000")
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            end_time = int(str(int(end_time.timestamp()))+"000")
            print({"time.timestamp": {"$gt":start_time,"$lte":end_time}})
            # cursor = mongo_collection.find({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
            cursor = mongo_collection.find({"time.timestamp": {"$gt":start_time,"$lte":end_time}})
            list_cur = list(cursor)
            return list_cur
        else:
            print({"time.UTC_time": {"$lte":end_time}})
            # cursor = mongo_collection.find({"time.UTC_time": {"$lte":end_time}})
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            end_time = int(str(int(end_time.timestamp()))+"000")
            # cursor = mongo_collection.find({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
            cursor = mongo_collection.find({"time.timestamp": {"$lte":end_time}})
            list_cur = list(cursor)
            return list_cur
    # def get_data(mongo_collection,start_time=None, end_time=None):
    #     print("in mongo data")
    #     if start_time and end_time:
    #         print({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
    #         cursor = mongo_collection.find({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
    #         list_cur = list(cursor)
    #         return list_cur
    #     else:
    #         print({"time.UTC_time": {"$lte":end_time}})
    #         cursor = mongo_collection.find({"time.UTC_time": {"$lte":end_time}})
    #         list_cur = list(cursor)
    #         return list_cur
    def get_hourlydata(mongo_collection,camera_id, usecase_id, start_time=None, end_time=None):
        if start_time and end_time:
            print({"time.UTC_time": {"$gt":start_time,"$lte":end_time}, "hierarchy.camera_id":camera_id,"usecase.usecase_id":usecase_id})
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            start_time = int(str(int(start_time.timestamp()))+"000")
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            end_time = int(str(int(end_time.timestamp()))+"000")
            print({"time.timestamp": {"$gt":start_time,"$lte":end_time}})
            mongo_query={"time.timestamp": {"$gt":start_time,"$lte":end_time}, "hierarchy.camera_id":camera_id,"usecase.usecase_id":usecase_id}
            cursor = mongo_collection.find(mongo_query)
            list_cur = list(cursor)
            return list_cur
        else:
            print({"time.UTC_time": {"$lte":end_time}})
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            end_time = int(str(int(end_time.timestamp()))+"000")
            cursor = mongo_collection.find({"time.timestamp": {"$lte":end_time},"hierarchy.camera_id":camera_id,"usecase.usecase_id":usecase_id})
            list_cur = list(cursor)
            return list_cur
        
    def get_shiftbaseddata(mongo_collection,camera_id, usecase_id, start_time=None, end_time=None):
        if start_time and end_time:
            print({"time.timestamp": {"$gt":start_time,"$lte":end_time}, "hierarchy.camera_id":camera_id,"usecase.usecase_id":usecase_id})
            mongo_query={"time.timestamp": {"$gt":start_time,"$lte":end_time}, "hierarchy.camera_id":camera_id,"usecase.usecase_id":usecase_id}
            cursor = mongo_collection.find(mongo_query)
            list_cur = list(cursor)
            return list_cur
        else:
            print({"time.timestamp": {"$lte":end_time}})
            cursor = mongo_collection.find({"time.timestamp": {"$lte":end_time},"hierarchy.camera_id":camera_id,"usecase.usecase_id":usecase_id})
            list_cur = list(cursor)
            return list_cur
class Sql_Data:
    def get_data(url):
        response=requests.get(url)
        result = response.json()['data'][0]
        # print(result)
        return (result['start_time'], result['end_time'])
    
    def update_data(url, start_time, end_time):
        print()
        response=requests.post(url,json={"start_time":start_time,"end_time":end_time})
        print(response.json())
        return response.json()
        
        
        