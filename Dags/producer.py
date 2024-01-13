
def get_data():
    import requests
    import time
    URL = 'http://localhost:8000'
    curr = time.time()
    while time.time < curr+60 : #produce for a minute and airflow will run the script every minute 
        try:
            response = requests.get(URL)
            if response.status_code == 200:
                print("get request: passed")
                # send to kafka
                # print('send to kafka: passed)
        except:
            continue

def send2kafka(data):
    pass