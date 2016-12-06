# encoding=utf-8
from pymongo import MongoClient
from pprint import pprint
import urllib.request
import urllib.parse
import json

Data_set = input("請輸入資料集編號: ")
Location_name = input("請輸入地點(預設全選): ")
target_url = urllib.request.Request('http://opendata.cwb.gov.tw/api/v1/rest/datastore/'+Data_set+'?locationName=' + urllib.parse.quote(Location_name, safe=''))
with open("token","r") as token_file:
    token = token_file.readline().rstrip('\n')
    print(token)
target_url.add_header( 'Authorization' , token)
fp = urllib.request.urlopen(target_url)
pure_data = json.loads(fp.read().decode('utf-8'))
pprint(pure_data)


fp.close()
