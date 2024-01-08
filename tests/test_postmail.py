import requests
import json

url = "http://172.16.0.170:7575/api/v1/mailservice/email/notification"
payload = {
    "notification_id": 1,
    "total_count": 1,
    "params": [
        {
            "usecase_id": "1",
            "usecase_name": "Pathway",
            "incident_id": "1",
            "incident_name": "Pathway Violation",
            "camera_id": 1,
            "camera_name": "CAM_03",
            "incident_count": 1
        },
        {
            "usecase_id": "2",
            "usecase_name": "Pathway2",
            "incident_id": "1",
            "incident_name": "Pathway Violation2",
            "camera_id": 1,
            "camera_name": "CAM_032",
            "incident_count": 1
        }
    ]
}

headers = {'Content-Type': 'application/json'}
try:
    print(f"url: {url}")
    print("json dumps: ",json.dumps(payload))
    r = requests.request("POST", url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})
    print(r.text)
    # r = requests.post(url, data=res)
    # print(f"Status Code: {r.status_code}, Response: {r.json()}")
except Exception as e:
    print(f"exception raised {e}")

# import requests
# import json
 
# url = "http://172.16.0.170:7575/api/v1/mailservice/email/notification"
 

# res = {
#   "notification_id": 1,
#   "total_count": 1,
#   "params": [
#     {
#       "usecase_id": "1",
#       "usecase_name": "Pathway",
#       "incident_id": "1",
#       "incident_name": "Pathway Violation",
#       "camera_id": 1,
#       "camera_name": "CAM_03",
#       "incident_count": 1
#     },
#     {
#       "usecase_id": "2",
#       "usecase_name": "Pathway2",
#       "incident_id": "1",
#       "incident_name": "Pathway Violation2",
#       "camera_id": 1,
#       "camera_name": "CAM_032",
#       "incident_count": 1
#     }
#   ]
# }




# headers = {
#   'Content-Type': 'application/json'
# }
 
# response = requests.request("POST", url, headers=headers, data=json.dumps(res))
 
# print(response.text)