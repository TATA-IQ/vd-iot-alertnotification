import pandas as pd
from datetime import datetime

class create_dataframe:
    def __init__(self,):
        return
        
    def incidentlist(self, masters, incidents):
        final_list = []
        for w in incidents:
            incident = []
            if "misc" in w:
                d = 0
                t = 0
                for data_lbl in w["misc"]:
                    if("data" in data_lbl): ## text in number plate 
                        d=d+int(data_lbl["data"])
                    if("text" in data_lbl):
                        t+=1 
                temp1 = [w["incident_id"], w["name"], d, t]
                temp = masters + temp1 
            else:
                temp1 = [w["incident_id"], w["name"], 0, 0]
                temp = masters + temp1
            final_list.append(temp)
        return final_list

  
    
    def convert_mongo_to_db(self,list_cur):
        df_all = pd.DataFrame(columns =['camera_id', 'camera_name','usecase_id', 
                                        'usecase_name','incident_id', 'incident_name',
                                        'incidnt_value', 'incident_label'])
        for data in list_cur:
            masters = [data["hierarchy"]["camera_id"],data["hierarchy"]["camera_name"], 
                       data["usecase"]["usecase_id"], data["usecase"]["name"]]
            final_list = self.incidentlist(masters, data['incident'])
            df = pd.DataFrame(final_list, columns =['camera_id', 'camera_name','usecase_id', 
                                                    'usecase_name','incident_id', 'incident_name', 
                                                    'incidnt_value', 'incident_label'])
            df_all=pd.concat([df_all, df])
        return df_all
    
    @staticmethod
    def summarization(df_all):
        # df_all.to_csv("df_all.csv")
        summary = df_all.groupby(['camera_id', 'camera_name','usecase_id', 
                                  'usecase_name','incident_id', 'incident_name']).agg({'incident_id': ['count']})
        s = summary.reset_index()

        s.columns = ['camera_id', 'camera_name','usecase_id', 
                                  'usecase_name','incident_id', 'incident_name','incident_count']
        # s.columns = ['camera_id', 'camera_name', 'zone_id', 'zone_name', 'subsite_id', 'subsite_name', 'city_id','city_name',
        #                     'location_id', 'location_name', 'customer_id', 'customer_name', 'usecase_id', 'usecase_name',
        #                     'incident_date_local', 'incident_hour_local', 'incident_date_gmt', 'incident_hour_gmt',
        #                     'incident_id', 'incident_name', 'incident_count','incident_label_count','incident_val_sum', 'incident_val_min','incident_val_max', 'incident_val_avg']

        s.head()

        return s