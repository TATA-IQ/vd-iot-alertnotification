from datetime import datetime, timedelta
import time

while True:
    current_time = datetime.now()
    print("current_time", current_time)
    if current_time.second >= 5 and current_time.second <= 10:             
        start_time = datetime.now().replace(second=0)-timedelta(minutes=1) # # should be changed
        end_time = datetime.now().replace(second=0)   
        print(f"hey, running now for start_time {start_time} and end_time {end_time}")    

        
    time.sleep(5)