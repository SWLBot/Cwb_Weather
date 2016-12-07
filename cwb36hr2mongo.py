# cwb api type ID F-C0032-001
# save data to mongo
from pymongo import MongoClient
import json
import uuid

def F_C0032_001mongo(pure_data):
	#connect to mongo
	client = MongoClient("mongodb://localhost:27017/")
	db = client['nctu_bot']
	collect = db['weather']

	check_insert = 1
	check_data = 1
	deal_data = [{'_id' : '', 'city_county' : '', 'start_time' : '', 'end_time' : '', 'wx_value' : 0, 'wx_str' : '', 'pop_value' : 0, 'pop_unit' : '', 'ci_str' : '', 'mint_value' : 0, 'mint_unit' : '', 'maxt_value' : 0, 'maxt_unit' : '', 'update' : 0},
				 {'_id' : '', 'city_county' : '', 'start_time' : '', 'end_time' : '', 'wx_value' : 0, 'wx_str' : '', 'pop_value' : 0, 'pop_unit' : '', 'ci_str' : '', 'mint_value' : 0, 'mint_unit' : '', 'maxt_value' : 0, 'maxt_unit' : '', 'update' : 0},
				 {'_id' : '', 'city_county' : '', 'start_time' : '', 'end_time' : '', 'wx_value' : 0, 'wx_str' : '', 'pop_value' : 0, 'pop_unit' : '', 'ci_str' : '', 'mint_value' : 0, 'mint_unit' : '', 'maxt_value' : 0, 'maxt_unit' : '', 'update' : 0}]

	#reshape data structure
	for tt in range(len( pure_data['records']['location'] )):
		for tt0 in range(3):
			deal_data[tt0]['_id'] = str(uuid.uuid3(uuid.uuid1(), 'javascript'))
			#print (deal_data[tt0]['_id'])
			deal_data[tt0]['city_county'] =  pure_data['records']['location'][tt]['locationName']
			for tt1 in range(len( pure_data['records']['location'][tt]['weatherElement'] )):
				if pure_data['records']['location'][tt]['weatherElement'][tt1]['elementName'] == 'Wx':
					deal_data[tt0]['start_time'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['startTime']
					deal_data[tt0]['end_time'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['endTime']
					deal_data[tt0]['wx_value'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['parameter']['parameterValue']
					deal_data[tt0]['wx_str'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['parameter']['paramterName']
				elif pure_data['records']['location'][tt]['weatherElement'][tt1]['elementName'] == 'PoP':
					deal_data[tt0]['pop_value'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['parameter']['paramterName']
					deal_data[tt0]['pop_unit'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['parameter']['parameterUnit']
				elif pure_data['records']['location'][tt]['weatherElement'][tt1]['elementName'] == 'CI':
					deal_data[tt0]['ci_str'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['parameter']['paramterName']
				elif pure_data['records']['location'][tt]['weatherElement'][tt1]['elementName'] == 'MinT':
					deal_data[tt0]['mint_value'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['parameter']['paramterName']
					deal_data[tt0]['mint_unit'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['parameter']['parameterUnit']
				elif pure_data['records']['location'][tt]['weatherElement'][tt1]['elementName'] == 'MaxT':
					deal_data[tt0]['maxt_value'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['parameter']['paramterName']
					deal_data[tt0]['maxt_unit'] = pure_data['records']['location'][tt]['weatherElement'][tt1]['time'][tt0]['parameter']['parameterUnit']
				else :
					check_data = 0
					print ('data structure error')
					
		
		
		#save data and prevent double 
		for i in range(3):
			querry_data = collect.find({'city_county':deal_data[i]['city_county'],'start_time' : deal_data[i]['start_time']}).sort('update', -1).limit(1)
			if querry_data.count() != 0:
				if (querry_data[0]['start_time'] == deal_data[i]['start_time'] and
					querry_data[0]['end_time'] == deal_data[i]['end_time'] and
					querry_data[0]['wx_value'] == deal_data[i]['wx_value'] and
					querry_data[0]['wx_str'] == deal_data[i]['wx_str'] and
					querry_data[0]['pop_value'] == deal_data[i]['pop_value'] and
					querry_data[0]['pop_unit'] == deal_data[i]['pop_unit'] and
					querry_data[0]['ci_str'] == deal_data[i]['ci_str'] and
					querry_data[0]['mint_value'] == deal_data[i]['mint_value'] and
					querry_data[0]['mint_unit'] == deal_data[i]['mint_unit'] and
					querry_data[0]['maxt_value'] == deal_data[i]['maxt_value'] and
					querry_data[0]['maxt_unit'] == deal_data[i]['maxt_unit']):
					#print("data ", end="", flush=True)
					#print(i, end="", flush=True)
					#print(" is exist")
					continue
				else :
					deal_data[i]['update'] = querry_data[0]['update'] + 1
			
			insert_id = collect.insert_one(deal_data[i]).inserted_id
			if insert_id is None:
				check_insert = 0
				print('error insert data to mongo')
				

	client.close()


	if check_data == 0 and check_insert == 0:
		return -2
	elif check_data == 0 and check_insert == 1:
		return -3
	elif check_data == 1 and check_insert == 0:
		return -1
	else :
		return 1