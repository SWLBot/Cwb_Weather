# encoding=utf-8
from pprint import pprint
from cwb36hr2mongo import F_C0032_001mongo
import urllib.request
import urllib.parse
import json

Data_set = input("請輸入資料集編號: ")
Location_name = input("請輸入地點(預設全選): ")

#connect to cwb api
urll = 'http://opendata.cwb.gov.tw/api/v1/rest/datastore/'+Data_set+'?'
Location_name = Location_name.strip()
if Location_name:
	urll = urll+'locationName='
	target_url = urllib.request.Request(urll+ urllib.parse.quote(Location_name, safe='')+'&sort=time')
else:
	target_url = urllib.request.Request(urll+ urllib.parse.quote(Location_name, safe='')+'sort=time')

with open("token","r") as token_file:
    token = token_file.readline().rstrip('\n')
    #print(token)
target_url.add_header( 'Authorization' , token)
fp = urllib.request.urlopen(target_url)
pure_data = json.loads(fp.read().decode('utf-8'))
#pprint(pure_data)

fp.close()

#save to mongo
result = F_C0032_001mongo(pure_data)
if result == -2:
	print ('data structure and mongo insert error!')
elif result == -3:
	print ('data structure and mongo data error!')
elif result == -1:
	print ('mongo insert error!')
else :
	print ('data insert to mongo success!')