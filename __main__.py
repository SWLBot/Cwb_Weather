# encoding=utf-8
from weather import grab_data

def main():
    data = grab_data()
    for location in data.keys():
        for time in data[location].keys():
            if(int(data[location][time]['MaxT']) < 20):
                print(location)
                print("\t"+time+" --- "+data[location][time]['endTime'] +"  "+data[location][time]['CI'])


main()
