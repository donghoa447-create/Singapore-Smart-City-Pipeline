import requests 
import os

url= "https://datamall2.mytransport.sg/ltaodataservice/v4/TrafficSpeedBands"
key = os.environ["LTA_ACCOUNT_KEY"]

response = requests.get(url, headers={"AccountKey": key})
data = response.json()

print(len(data["value"])) # how many roads records came back 
print(data["value"][0]) # print the first record