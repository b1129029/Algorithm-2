import zipfile
import requests
import json
from datetime import datetime, timedelta
import urllib.request
import zipfile
import os
import json
import io
from collections import defaultdict

from SQL import MySQLDatabase, insertManagerDataInToSql, insertRecordViewDataInToSql, insertTripDataInToSql, insertTravelDataInToSql, insertUserDataInToSql, insertRouteDataInToSql, insertRecordTravelDataInToSql, insertRecordTripDataInToSql
from SQL import updateManagerDataInToSql, updateUserDataInToSql, updateCityDataInToSql, updateSiteDataInToSql, updateHotelDataInToSql, updateRestaurantDataInToSql, updateViewDataInToSql, updateActivityDataInToSql, updateRouteDataInToSql, updateTripDataInToSql, updateRecordTripDataInToSql, updateTravelDataInToSql, updateRecordTravelDataInToSql, updateRecordViewDataInToSql, update36hoursDataInToSql, update2daysDataInToSql, updateweekDataInToSql
from trip import Travel, Trip

FILENAME = 'testdata1'
DB = 'db\\' + FILENAME + '.db'
TABLE = 'my_table'

HOST = '127.0.0.1'
USER = 'root'
PASSWORD = ''
DATABASENAME = 'city'

city_query_create = """CREATE TABLE `city` (
  `city_id` varchar(127)
) """

city_query_insert = """INSERT INTO city (city_id) VALUES (%s)"""

site_query_create = """CREATE TABLE `site` (
  `city_id` varchar(127),
  `site_id` varchar(127)
) """
site_query_insert = """INSERT INTO site (city_id, site_id) VALUES (%s, %s)"""

class Crawler_Weather():
    def __init__(self):
        pass

    def get_data_36hours(self):
        file = '/v1/opendataapi/F-C0032-001'
        Authorization = 'Authorization=CWA-18EE94ED-C159-46EB-87F1-7071CF6612FE'
        url = 'https://opendata.cwa.gov.tw/fileapi'+ file +'?' + Authorization + '&downloadType=WEB&format=JSON'
        print(url)

        data = requests.get(url)   # 取得 JSON 檔案的內容為文字
        data_json = data.json()    # 轉換成 JSON 格式
        processed_locations = set()  # To keep track of processed locations

        fields = ['城市', '開始日期', '開始時間', '結束日期','結束時間', '天氣現象', '最高溫度', '最低溫度', '舒適度指數', '降雨機率']
        tmpList = []
        location = data_json['cwaopendata']['dataset']['location']
        for i in location:
            city = i['locationName']    # 縣市名稱

            for k in range(len(i['weatherElement'][0]['time'])):
                tmp = [city]
                date, time = return_date_time(i['weatherElement'][0]['time'][k]['startTime'])
                date2, time2 = return_date_time(i['weatherElement'][0]['time'][k]['endTime'])
                tmp.append(date)
                tmp.append(time)
                tmp.append(date2)
                tmp.append(time2)
                
                for j in range(len(i['weatherElement'])):
                    tmp.append(i['weatherElement'][j]['time'][k]['parameter']['parameterName'])
                tmpList.append(tmp)
            
        return fields, tmpList


    def get_data_2Days(self):
        #file type 2
        Authorization = 'Authorization=CWA-18EE94ED-C159-46EB-87F1-7071CF6612FE'
        fields = ['城市', '鄉鎮', '開始日期', '開始時間', '結束日期','結束時間', '溫度', '露點溫度', '相對濕度', '6小時降雨機率','12小時降雨機率','風向', '風速', '舒適度指數', '體感溫度', '天氣現象', '天氣預報綜合描述']
        tmpList = []
        OFFSET = 6

        for idx in range(23):#23
            fileName = 'F-D0047-'
            if (idx*4+1) < 10:
                fileName += '00'
                fileName += str((idx*4+1))
            else:
                fileName += '0'
                fileName += str((idx*4+1))

            url = 'https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/'+ fileName +'?' + Authorization + '&downloadType=WEB&format=JSON'
            print(url)

            data = requests.get(url)   # 取得 JSON 檔案的內容為文字
            data_json = data.json()    # 轉換成 JSON 格式
            locations = data_json['cwaopendata']['dataset']['locations']['location']
            LOCATIONSNAME = data_json['cwaopendata']['dataset']['locations']['locationsName']

            if LOCATIONSNAME == '台灣':
                continue
            
            for location in locations:
                city = LOCATIONSNAME    # 縣市名稱
                site = location['locationName']

                for i in range(31):
                    temp = []
                    date1, time1 = return_date_time(location['weatherElement'][9]['time'][i]['startTime'])
                    date2, time2 = return_date_time(location['weatherElement'][9]['time'][i]['endTime'])
                    temp.append(city)
                    temp.append(site)
                    temp.append(date1)
                    temp.append(time1)
                    temp.append(date2)
                    temp.append(time2)

                    for j in range(11):
                        if j == 3:
                            measure = '%' if location['weatherElement'][j]['time'][int(i/2)]['elementValue']['measures'] == '百分比' else location['weatherElement'][j]['time'][int(i/j)]['elementValue']['measures']
                            temp.append(location['weatherElement'][j]['time'][int(i/j)]['elementValue']['value']+ " "+measure)
                        if j == 4:
                            measure = '%' if location['weatherElement'][j]['time'][int(i/4)]['elementValue']['measures'] == '百分比' else location['weatherElement'][j]['time'][int(i/j)]['elementValue']['measures']
                            temp.append(location['weatherElement'][j]['time'][int(i/j)]['elementValue']['value']+ " "+measure)
                        if j in [0, 1, 2, 5, 8]:
                            measure = '%' if location['weatherElement'][j]['time'][i]['elementValue']['measures'] == '百分比' else location['weatherElement'][j]['time'][i]['elementValue']['measures']
                            temp.append(location['weatherElement'][j]['time'][i]['elementValue']['value']+ " "+measure)
                        if j == 10:
                            temp.append(location['weatherElement'][j]['time'][i]['elementValue']['value'])
                        if j == 6:
                            measure = '%' if location['weatherElement'][j]['time'][i]['elementValue'][0]['measures'] == '百分比' else location['weatherElement'][j]['time'][i]['elementValue'][0]['measures']
                            temp.append(location['weatherElement'][j]['time'][i]['elementValue'][0]['value']+ " "+measure)
                        if j == 9:
                            temp.append(location['weatherElement'][j]['time'][i]['elementValue'][0]['value'])
                        if j == 7:
                            temp.append(location['weatherElement'][j]['time'][i]['elementValue'][1]['value'])

                    #print(temp)
                    tmpList.append(temp)
                
        return fields, tmpList

    def get_data_Week(self):
        # type 3
        Authorization = 'Authorization=CWA-B7166A49-DAA0-489F-913D-F6593E39785D'
        fields = ['城市', '鄉鎮', '開始日期', '開始時間', '結束日期','結束時間','平均溫度', '平均露點溫度', '平均相對濕度', '最高溫度', '最低溫度', '最高體感溫度', '最低體感溫度', '最大舒適度指數', '最小舒適度指數', '12小時降雨機率', '風向', '最大風速', '天氣現象', '紫外線指數', '天氣預報綜合描述']
        tmpList = []
        processed_locations = set()  # To keep track of processed locations

        for idx in range(23):
            file_name = 'F-D0047-'
            file_name += '00' + str(idx*4+3) if (idx*4+3) < 10 else '0' + str(idx*4+3)
            url = 'https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/' + file_name + '?' + Authorization + '&downloadType=WEB&format=JSON'
            data = requests.get(url)
            data_json = data.json()
            locations = data_json['cwaopendata']['dataset']['locations']['location']
            location_name = data_json['cwaopendata']['dataset']['locations']['locationsName']
            print(url)

            if location_name == '台灣':
                continue

            for location in locations:
                city = location_name
                site = location['locationName']

                for i in range(14):
                    
                    temp = []
                    date1, time1 = return_date_time(location['weatherElement'][9]['time'][i]['startTime'])
                    date2, time2 = return_date_time(location['weatherElement'][9]['time'][i]['endTime'])
                    temp.append(city)
                    temp.append(site)
                    temp.append(date1)
                    temp.append(time1)
                    temp.append(date2)
                    temp.append(time2)

                    for j in range(15):
                        if j == 13:
                            temp.append(location['weatherElement'][j]['time'][int(i/j)]['elementValue'][1]['value'])
                        if j in [0, 1, 2, 3, 4, 5, 6, 9, 10]:
                            measure = '%' if location['weatherElement'][j]['time'][i]['elementValue']['measures'] == '百分比' else location['weatherElement'][j]['time'][i]['elementValue']['measures']
                            temp.append(str(location['weatherElement'][j]['time'][i]['elementValue']['value']) + " " + measure)
                        if j == 14:
                            temp.append(str(location['weatherElement'][j]['time'][i]['elementValue']['value']))
                        if j in [7, 8]:
                            temp.append(location['weatherElement'][j]['time'][i]['elementValue'][1]['value'])
                        if j == 11:
                            temp.append(location['weatherElement'][j]['time'][i]['elementValue'][0]['value'] + " " + location['weatherElement'][j]['time'][i]['elementValue'][0]['measures'])
                        if j == 12:
                            temp.append(location['weatherElement'][j]['time'][i]['elementValue'][0]['value'])
                    #print(temp)
                    tmpList.append(temp)

        return fields, tmpList

class Crawler_Hotel():
    def __init__(self):
        pass

    def get_data_Hotel(self):
        hotel_file_path_main  = './opensource/hotel/HotelList.json'
        with io.open(hotel_file_path_main, 'r', encoding='utf-8-sig') as file:
                hotel_main = json.load(file)
                file.close()
        fields = ['ID', '城市', '鄉鎮', '地址', '名字', '旅館類別', '描述', '網址', '電話', '服務資訊', '房型', '停車資訊', '照片1', '照片描述1', '照片2', '照片描述2', '照片3', '照片描述3', '最低價格', '最高價格', '可容納人數', '經度', '緯度']
        #Hotel
        empty_field = {field: None for field in fields}
        tmpList = []
        for Hotel in hotel_main['Hotels']:
            empty_field = {field: None for field in fields}
            empty_field['ID'] = Hotel['HotelID']
            empty_field['旅館類別'] = Hotel['HotelClasses'][0]
            empty_field['城市'] = Hotel['PostalAddress']['City']
            empty_field['鄉鎮'] = Hotel['PostalAddress']['Town']
            empty_field['地址'] = Hotel['PostalAddress']['StreetAddress']
            empty_field['名字'] = Hotel['HotelName']
            empty_field['網址'] = Hotel['WebsiteURL']
            empty_field['描述'] = Hotel['Description']

            empty_field['服務資訊'] = Hotel['ServiceInfo']
            empty_field['停車資訊'] = Hotel['ParkingInfo']
            empty_field['最低價格'] = Hotel['LowestPrice']
            empty_field['最高價格'] = Hotel['CeilingPrice']

            empty_field['經度'] = Hotel['PositionLon']
            empty_field['緯度'] = Hotel['PositionLat']

            empty_field['可容納人數'] = Hotel['TotalCapacity']
            empty_field['房型'] = Hotel['RoomInfo']


            try:
                empty_field['電話'] = Hotel['Telephones'][0]['Tel']
            except:
                empty_field['電話'] = None
            try:
                empty_field['電話'] = Hotel['Telephones'][0]['Tel'] + Hotel['Telephone'][0]['Ext']
            except:
                empty_field['電話'] = None
            try:
                empty_field['電話'] = Hotel['Telephones'][0]['Tel']
            except:
                empty_field['電話'] = None
    

            try:
                empty_field['照片1'] = Hotel['Images'][0]['URL']
                empty_field['照片描述1'] = Hotel['Images'][0]['Name']
            except:
                empty_field['照片1'] = None
                empty_field['照片描述1'] = None

            try:
                empty_field['照片2'] = Hotel['Images'][1]['URL']
                empty_field['照片描述2'] = Hotel['Images'][1]['Name']
            except:
                empty_field['照片2'] = None
                empty_field['照片描述2'] = None

            try:
                empty_field['照片3'] = Hotel['Images'][2]['URL']
                empty_field['照片描述3'] = Hotel['Images'][2]['Name']
            except:
                empty_field['照片3'] = None
                empty_field['照片描述3'] = None

            

            tmpList.append(empty_field)


        # Convert merged_dict to a list of lists including IDs, preserving the order
        merged_list_with_ids = []
        for tmp in tmpList:
            merged_list_with_ids.append(list(tmp.values()))

        Realfields = ['ID', '城市', '鄉鎮', '地址', '名字', '旅館類別', '描述', '網址', '電話', '服務資訊', '房型', '停車資訊', '照片1', '照片描述1', '照片2', '照片描述2', '照片3', '照片描述3', '最低價格', '最高價格', '可容納人數', '經度', '緯度']


        return Realfields, merged_list_with_ids


    def get_data_Hotel_old(self):
        url = 'https://media.taiwan.net.tw/XMLReleaseALL_public/hotel_C_f.json'
        fields = ['城市', '鄉鎮', '地址', 'ID', '名字', '網址', '電話', '服務資訊', '停車資訊', '照片1', '照片描述1', '照片2', '照片描述2', '照片3', '照片描述3', '最低價格', '最高價格', '電子郵件', '可容納人數', '經度', '緯度']
        tmpList = []
        data = requests.get(url)
        data.encoding = 'utf-8-sig'
        data_json = data.json()
        info = data_json['XML_Head']['Infos']['Info']
        processed_ids = set()  # To keep track of processed id
        for i in info :
            id = i['Id']
            if id not in processed_ids:
                city = i['Region']
                site = i['Town']
                add = i['Add']
                name = i['Name']
                web = i['Website']
                tex = i['Tel']
                serviceinfo = i['Serviceinfo']
                parkinginfo = i['Parkinginfo']
                picture1 = i['Picture1']
                pic_describe1 =i['Picdescribe1']
                picture2 = i['Picture2']
                pic_describe2 =i['Picdescribe2']
                picture3 = i['Picture3']
                pic_describe3 =i['Picdescribe3']
                lowestprice = i['LowestPrice']
                lowestprice = i['LowestPrice']
                ceilingprice = i['CeilingPrice']
                people = i['TotalNumberofPeople']
                industryemail = i['IndustryEmail']
                Px = i['Px']
                Py = i['Py']
                tmpList.append([city, site, add, id, name, web, tex, serviceinfo, parkinginfo, picture1, pic_describe1, picture2, pic_describe2, picture3, pic_describe3, lowestprice , ceilingprice, industryemail, people, Px, Py])
                processed_ids.add(id)

        return fields, tmpList


class Crawler_View():
    def __init__(self):
        pass

    def get_data_view(self):
        attraction_file_path_main  = './opensource/attraction/AttractionList.json'
        attraction_file_path_fee  = './opensource/attraction/AttractionFeeList.json'
        attraction_file_path_Service_time  = './opensource/attraction/AttractionServiceTimeList.json'
        #load data
        with io.open(attraction_file_path_main, 'r', encoding='utf-8-sig') as file:
            attraction_main = json.load(file)
            file.close()

        with io.open(attraction_file_path_fee, 'r', encoding='utf-8-sig') as file:
            attraction_fee = json.load(file)
            file.close()

        with io.open(attraction_file_path_Service_time, 'r', encoding='utf-8-sig') as file:
            attraction_Service_time = json.load(file)
            file.close()
        

        
        #Attraction
        fields = ['ID', '城市', '鄉鎮', '郵遞區號', '地址', 
                  '名字', '網址', '描述', '電話', '旅遊資訊', 
                  '停車資訊', '照片1', '照片描述1', '照片2', '照片描述2', 
                  '照片3', '照片描述3', '經度', '緯度', '景點類型1',
                  '景點類型2', '景點類型3', '最低價格', '最高價格', '詳細價格描述',
                  '服務時間']
        empty_field = {field: None for field in fields}
        tmpList = []
        for Attraction in attraction_main['Attractions']:
            empty_field = {field: None for field in fields}

            empty_field['ID'] = Attraction['AttractionID']

            empty_field['城市'] = Attraction['PostalAddress']['City']
            empty_field['鄉鎮'] = Attraction['PostalAddress']['Town']
            empty_field['地址'] = Attraction['PostalAddress']['StreetAddress']
            empty_field['名字'] = Attraction['AttractionName']
            empty_field['網址'] = Attraction['WebsiteURL']
            empty_field['描述'] = Attraction['Description']

            empty_field['郵遞區號'] = Attraction['PostalAddress']['ZipCode']


            try:
                empty_field['電話'] = Attraction['Telephones'][0]['Tel']
            except:
                empty_field['電話'] = None
            try:
                empty_field['電話'] = Attraction['Telephones'][0]['Tel'] + Attraction['Telephone'][0]['Ext']
            except:
                empty_field['電話'] = None
                

            empty_field['停車資訊'] = Attraction['ParkingInfo']
            empty_field['旅遊資訊'] = Attraction['TrafficInfo']

            try:
                empty_field['景點類型1'] = Attraction['AttractionClasses'][0]
            except:
                empty_field['景點類型1'] = None

            try:
                empty_field['景點類型2'] = Attraction['AttractionClasses'][1]
            except:
                empty_field['景點類型2'] = None

            try:
                empty_field['景點類型3'] = Attraction['AttractionClasses'][2]
            except:
                empty_field['景點類型3'] = None

            try:
                empty_field['照片1'] = Attraction['Images'][0]['URL']
                empty_field['照片描述1'] = Attraction['Images'][0]['Name']
            except:
                empty_field['照片1'] = None
                empty_field['照片描述1'] = None

            try:
                empty_field['照片2'] = Attraction['Images'][1]['URL']
                empty_field['照片描述2'] = Attraction['Images'][1]['Name']
            except:
                empty_field['照片2'] = None
                empty_field['照片描述2'] = None

            try:
                empty_field['照片3'] = Attraction['Images'][2]['URL']
                empty_field['照片描述3'] = Attraction['Images'][2]['Name']
            except:
                empty_field['照片3'] = None
                empty_field['照片描述3'] = None


            empty_field['經度'] = Attraction['PositionLon']
            empty_field['緯度'] = Attraction['PositionLat']

            tmpList.append(empty_field)

        #AttractionFee
        fields = ['ID', '最高價格', '最低價格', '詳細價格描述']
        empty_field = {field: None for field in fields}
        tmpList2 = []
        for AttractionFee in attraction_fee['AttractionFees']:
            empty_field = {field: None for field in fields}
            empty_field['ID'] = AttractionFee['AttractionID']
            desStr = ''
            minPrice = 9999999999
            maxPrice = -1
            for fee in AttractionFee['Fees']:
                desStr += str(fee['Name']) + ' ' + str(fee['Description']) + ' :' + str(fee['Price']) + '\n'
                minPrice = min(minPrice, int(fee['Price']))
                maxPrice = max(maxPrice, int(fee['Price']))
            #empty_field['ID'] = AttractionFee['AttractionID']
            empty_field['最高價格'] = maxPrice
            empty_field['最低價格'] = minPrice
            empty_field['詳細價格描述'] = desStr
            tmpList2.append(empty_field)
        
        #AttractionServiceTime
        fields = ['ID', '服務時間']
        empty_field = {field: None for field in fields}
        tmpList3 = []
        for AttractionServiceTime in attraction_Service_time['AttractionServiceTimes']:
            empty_field['ID'] = AttractionServiceTime['AttractionID']
            desStr = ''
            for serviceTime in AttractionServiceTime['ServiceTimes']:
                desStr += serviceTime['Name'] + " (" + serviceTime['StartTime'] + " - " + serviceTime['EndTime'] + ")\n"
            empty_field['服務時間'] = desStr
            tmpList3.append(empty_field)

        

        # Convert merged_dict to a list of lists including IDs, preserving the order
        merged_list_with_ids = []
        for tmp in tmpList:
            if len(tmpList2) > 0 and tmp['ID'] == tmpList2[0]['ID']:
                tmp['最低價格'] = tmpList2[0]['最低價格']
                tmp['最高價格'] = tmpList2[0]['最高價格']
                tmp['詳細價格描述'] = tmpList2[0]['詳細價格描述']
                tmpList2.pop(0)
            if len(tmpList3) > 0 and tmp['ID'] == tmpList3[0]['ID']:
                tmp['服務時間'] = tmpList3[0]['服務時間']
                tmpList3.pop(0)
            merged_list_with_ids.append(list(tmp.values()))

        Realfields = ['ID', '城市', '鄉鎮', '郵遞區號', '地址', '名字', '網址', '描述', '電話', '旅遊資訊', '停車資訊', '照片1', '照片描述1', '照片2', '照片描述2', '照片3', '照片描述3', '經度', '緯度', '景點類型1', '景點類型2', '景點類型3',
                    '最高價格', '最低價格', '詳細價格描述',
                        '服務時間']


        return Realfields, merged_list_with_ids

    def get_data_view_old(self):
        url = 'https://media.taiwan.net.tw/XMLReleaseALL_public/scenic_spot_C_f.json'
        fields = ['城市', '鄉鎮', '郵遞區號', '地址', 'ID', '名字', '描述', '簡述', '電話', '旅遊資訊', '開放時間', '照片1', '照片描述1', '照片2', '照片描述2', '照片3', '照片描述3', '經度', '緯度', '景點類別', '類別1', '類別2', '類別3', '古蹟分級', '網址', '停車資訊', '停車場經度', '停車場緯度', '票價', '註記', '關鍵字']
        tmpList = []
        data = requests.get(url)
        data.encoding = 'utf-8-sig'
        data_json = data.json()
        info = data_json['XML_Head']['Infos']['Info']
        processed_ids = set()  # To keep track of processed id

        for i in info :
            id = i['Id']
            if id not in processed_ids:
                city = i['Region']
                site = i['Town']
                zipcode = i['Zipcode']
                add = i['Add']
                name = i['Name']
                toldescribe = i['Toldescribe']
                description = i['Description']
                tel = i['Tel']
                travellinginfo = i['Travellinginfo']
                time = i['Opentime']
                picture1 = i['Picture1']
                pic_describe1 =i['Picdescribe1']
                picture2 = i['Picture2']
                pic_describe2 =i['Picdescribe2']
                picture3 = i['Picture3']
                pic_describe3 =i['Picdescribe3']
                Px = i['Px']
                Py = i['Py']
                orgclass = i['Orgclass']
                class1 = i['Class1'] #01.文化類、02.生態類、03.古蹟類、04.廟宇類、05.藝術類、06.小吃/特產類、07.國家公園類、08.國家風景區類、09.休閒農業類、10.溫泉類、11.自然風景類、12.遊憩類、13.體育健身類、14.觀光工廠類、15.都會公園類、16.森林遊樂區類、17.林場類、18.其他
                class2 = i['Class2'] #01.文化類、02.生態類、03.古蹟類、04.廟宇類、05.藝術類、06.小吃/特產類、07.國家公園類、08.國家風景區類、09.休閒農業類、10.溫泉類、11.自然風景類、12.遊憩類、13.體育健身類、14.觀光工廠類、15.都會公園類、16.森林遊樂區類、17.林場類、18.其他
                class3 = i['Class3'] #01.文化類、02.生態類、03.古蹟類、04.廟宇類、05.藝術類、06.小吃/特產類、07.國家公園類、08.國家風景區類、09.休閒農業類、10.溫泉類、11.自然風景類、12.遊憩類、13.體育健身類、14.觀光工廠類、15.都會公園類、16.森林遊樂區類、17.林場類、18.其他
                level = i['Level'] #古蹟分級 1.一級、2.二級、3.三級、4.國定、5.直轄市定、6.縣(市)定、9.非古蹟
                web = i['Website']
                parkinfo = i['Parkinginfo']
                park_Px = i['Parkinginfo_Px']
                park_Py = i['Parkinginfo_Py']
                ticket = i['Ticketinfo']
                remarks = i['Remarks']
                key = i['Keyword']
                tmpList.append([city, site, zipcode, add, id, name, toldescribe, description, tel, travellinginfo, time, picture1, pic_describe1, picture2, pic_describe2, picture3, pic_describe3, Px, Py, orgclass, class1, class2, class3, level, web, parkinfo, park_Px, park_Py, ticket, remarks, key])
                processed_ids.add(id)
        return fields, tmpList


class Crawler_Activity():
    def __init__(self):
        pass

    def get_data_Activity(self):
        event_file_path_main  = './opensource/event/EventList.json'
        with io.open(event_file_path_main, 'r', encoding='utf-8-sig') as file:
            event_main = json.load(file)
            file.close()
        fields = ['ID', '城市', '鄉鎮', '地址', '名字', 
                  '網址', '簡述', '電話', '主辦單位', '停車資訊', 
                  '照片1', '照片描述1', '照片2', '照片描述2', '照片3', 
                  '照片描述3', '類別1', '類別2', '類別3', '收費', 
                  '經度',  '緯度', '開始時間', '結束時間', '活動參與對象', 
                  '交通資訊']
        #Event
        empty_field = {field: None for field in fields}
        tmpList = []
        for Event in event_main['Events']:
            empty_field = {field: None for field in fields}

            empty_field['ID'] = Event['EventID']

            empty_field['城市'] = Event['PostalAddress']['City']
            empty_field['鄉鎮'] = Event['PostalAddress']['Town']
            empty_field['地址'] = Event['PostalAddress']['StreetAddress']
            empty_field['名字'] = Event['EventName']
            empty_field['網址'] = Event['WebsiteURL']
            empty_field['簡述'] = Event['Description']

            empty_field['停車資訊'] = Event['ParkingInfo']

            empty_field['經度'] = Event['PositionLon']
            empty_field['緯度'] = Event['PositionLat']
            empty_field['活動參與對象'] = Event['Participant']

            empty_field['開始時間'] = Event['StartDateTime']
            empty_field['結束時間'] = Event['EndDateTime']

            empty_field['交通資訊'] = Event['TrafficInfo']

            empty_field['收費'] = Event['FeeInfo']

            try:
                empty_field['主辦單位'] = Event['Organizations'][0]['Name']
            except:
                empty_field['主辦單位'] = None

            try:
                empty_field['類別1'] = Event['EventClasses'][0]
                if Event['EventClasses'][0] not in ['1', '2', '3', '4', '5', '9']:
                    empty_field['類別1'] = Event['EventClasses'][0][0]

            except:
                empty_field['類別1'] = None

            try:
                empty_field['類別2'] = Event['EventClasses'][1]
                if Event['EventClasses'][0] not in ['1', '2', '3', '4', '5', '9']:
                    empty_field['類別1'] = Event['EventClasses'][1][0]
            except:
                empty_field['類別2'] = None

            try:
                empty_field['類別3'] = Event['EventClasses'][2]
                if Event['EventClasses'][0] not in ['1', '2', '3', '4', '5', '9']:
                    empty_field['類別1'] = Event['EventClasses'][2][0]
            except:
                empty_field['類別3'] = None


            try:
                empty_field['電話'] = Event['Telephones'][0]['Tel']
            except:
                empty_field['電話'] = None
            try:
                empty_field['電話'] = Event['Telephones'][0]['Tel'] + Event['Telephone'][0]['Ext']
            except:
                empty_field['電話'] = None
            try:
                empty_field['電話'] = Event['Telephones'][0]['Tel']
            except:
                empty_field['電話'] = None


            try:
                empty_field['照片1'] = Event['Images'][0]['URL']
                empty_field['照片描述1'] = Event['Images'][0]['Name']
            except:
                empty_field['照片1'] = None
                empty_field['照片描述1'] = None

            try:
                empty_field['照片2'] = Event['Images'][1]['URL']
                empty_field['照片描述2'] = Event['Images'][1]['Name']
            except:
                empty_field['照片2'] = None
                empty_field['照片描述2'] = None

            try:
                empty_field['照片3'] = Event['Images'][2]['URL']
                empty_field['照片描述3'] = Event['Images'][2]['Name']
            except:
                empty_field['照片3'] = None
                empty_field['照片描述3'] = None

            

            tmpList.append(empty_field)

        # Convert merged_dict to a list of lists including IDs, preserving the order
        merged_list_with_ids = []
        for tmp in tmpList:
            merged_list_with_ids.append(list(tmp.values()))

        Realfields = ['ID', '城市', '鄉鎮', '地址', '名字', 
                        '網址', '簡述', '電話', '主辦單位', '停車資訊', 
                        '照片1', '照片描述1', '照片2', '照片描述2', '照片3', 
                        '照片描述3', '類別1', '類別2', '類別3', '收費', 
                        '經度',  '緯度', '開始時間', '結束時間', '活動參與對象', 
                        '交通資訊']
 
        return Realfields, merged_list_with_ids

    def get_data_Activity_old(self):
        url = 'https://media.taiwan.net.tw/XMLReleaseALL_public/activity_C_f.json'
        fields = ['城市', '鄉鎮', '地址', 'ID', '名字', '簡述', '活動參與對象', '電話', '主辦單位', '開始時間', '結束時間', '週期性活動', '非週期性活動', '照片1', '照片描述1', '照片2', '照片描述2', '照片3', '照片描述3', '經度', '緯度', '類別1', '類別2', '網址', '交通資訊', '停車資訊', '收費', '註記']
        tmpList = []
        data = requests.get(url)
        data.encoding = 'utf-8-sig'
        data_json = data.json()
        info = data_json['XML_Head']['Infos']['Info']
        processed_ids = set()  # To keep track of processed id

        for i in info :
            id = i['Id']
            if id not in processed_ids:
                city = i['Region']
                site = i['Town']
                add = i['Add']
                name = i['Name']
                description = i['Description']
                participation = i['Participation']
                tel = i['Tel']
                org = i['Org']
                time_start = i['Start']
                time_end = i['End']
                cycle = i['Cycle']
                noncycle = i['Noncycle']
                picture1 = i['Picture1']
                pic_describe1 =i['Picdescribe1']
                picture2 = i['Picture2']
                pic_describe2 =i['Picdescribe2']
                picture3 = i['Picture3']
                pic_describe3 =i['Picdescribe3']
                Px = i['Px']
                Py = i['Py']
                class1 = i['Class1'] #活動類型：01.節慶活動、02.藝文活動、03.年度活動、04.四季活動、05.產業文化活動、06.遊憩活動、07.活動快報、08.自行車活動、09.其他
                class2 = i['Class2'] #活動類型：01.節慶活動、02.藝文活動、03.年度活動、04.四季活動、05.產業文化活動、06.遊憩活動、07.活動快報、08.自行車活動、09.其他
                web = i['Website']
                travellinginfo = i['Travellinginfo']
                parkinginfo = i['Parkinginfo']
                charge = i['Charge']
                remarks = i['Remarks'] 
                tmpList.append([city, site, add, id, name, description, participation, tel, org, time_start, time_end, cycle, noncycle, picture1, pic_describe1, picture2, pic_describe2, picture3, pic_describe3, Px, Py, class1, class2, web, travellinginfo, parkinginfo, charge, remarks])
                processed_ids.add(id)
        return fields, tmpList


class Crawler_Restaurant():
    def __init__(self):
        pass

    def get_data_Restaurant(self):
        restaurant_file_path_main  = './opensource/restaurant/RestaurantList.json'
        restaurant_file_path_Service_time  = './opensource/restaurant/RestaurantServiceTimeList.json'
        with io.open(restaurant_file_path_main, 'r', encoding='utf-8-sig') as file:
            restaurant_main = json.load(file)
            file.close()

        with io.open(restaurant_file_path_Service_time, 'r', encoding='utf-8-sig') as file:
            restaurant_Service_time = json.load(file)
            file.close()
        #Restaurant
        fields = ['ID', '城市', '鄉鎮', '郵遞區號', '地址', 
                  '名字', '簡述', '電話', '照片1', '照片描述1', 
                  '照片2', '照片描述2', '照片3', '照片描述3', '餐廳類型1', 
                  '餐廳類型2', '餐廳類型3', '網址', '經度', '緯度',
                  '停車資訊', '服務時間']
        empty_field = {field: None for field in fields}
        tmpList = []
        for Restaurant in restaurant_main['Restaurants']:
        
            empty_field = {field: None for field in fields}
            empty_field['ID'] = Restaurant['RestaurantID']

            empty_field['城市'] = Restaurant['PostalAddress']['City']
            empty_field['鄉鎮'] = Restaurant['PostalAddress']['Town']
            empty_field['地址'] = Restaurant['PostalAddress']['StreetAddress']
            empty_field['郵遞區號'] = Restaurant['PostalAddress']['ZipCode']
            empty_field['名字'] = Restaurant['RestaurantName']
            empty_field['簡述'] = Restaurant['Description']
            empty_field['停車資訊'] = Restaurant['ParkingInfo']
            empty_field['網址'] = Restaurant['WebsiteURL']

            empty_field['經度'] = Restaurant['PositionLon']
            empty_field['緯度'] = Restaurant['PositionLat']


            try:
                empty_field['餐廳類型1'] = Restaurant['CuisineClasses'][0]
            except:
                empty_field['餐廳類型1'] = None

            try:
                empty_field['餐廳類型2'] = Restaurant['CuisineClasses'][1]
            except:
                empty_field['餐廳類型2'] = None

            try:
                empty_field['餐廳類型3'] = Restaurant['CuisineClasses'][2]
            except:
                empty_field['餐廳類型3'] = None



            try:
                empty_field['電話'] = Restaurant['Telephones'][0]['Tel']
            except:
                empty_field['電話'] = None
            try:
                empty_field['電話'] = Restaurant['Telephones'][0]['Tel'] + Restaurant['Telephone'][0]['Ext']
            except:
                empty_field['電話'] = None
            try:
                empty_field['電話'] = Restaurant['Telephones'][0]['Tel']
            except:
                empty_field['電話'] = None
                

            try:
                empty_field['照片1'] = Restaurant['Images'][0]['URL']
                empty_field['照片描述1'] = Restaurant['Images'][0]['Name']
            except:
                empty_field['照片1'] = None
                empty_field['照片描述1'] = None

            try:
                empty_field['照片2'] = Restaurant['Images'][1]['URL']
                empty_field['照片描述2'] = Restaurant['Images'][1]['Name']
            except:
                empty_field['照片2'] = None
                empty_field['照片描述2'] = None

            try:
                empty_field['照片3'] = Restaurant['Images'][2]['URL']
                empty_field['照片描述3'] = Restaurant['Images'][2]['Name']
            except:
                empty_field['照片3'] = None
                empty_field['照片描述3'] = None

            tmpList.append(empty_field)

        #resturantServiceTime
        fields = ['ID', '服務時間']
        empty_field = {field: None for field in fields}
        tmpList2 = []
        for RestaurantServiceTime in restaurant_Service_time['RestaurantServiceTimes']:
            desStr = ''
            empty_field = {field: None for field in fields}
            empty_field['ID'] = RestaurantServiceTime['RestaurantID']
            for serviceTime in RestaurantServiceTime['ServiceTimes']:
                desStr += serviceTime['Name'] + " (" + serviceTime['StartTime'] + " - " + serviceTime['EndTime'] + ")\n"
            empty_field['服務時間'] = desStr
            tmpList2.append(empty_field)

        # Convert merged_dict to a list of lists including IDs, preserving the order
        merged_list_with_ids = []
        for tmp in tmpList:
            if len(tmpList2) > 0 and tmp['ID'] == tmpList2[0]['ID']:
                tmp['服務時間'] = tmpList2[0]['服務時間']
                tmpList2.pop(0)
                
            merged_list_with_ids.append(list(tmp.values()))

        Realfields = ['ID', '城市', '鄉鎮', '郵遞區號', '地址', '名字', '簡述', '電話', '照片1', '照片描述1', '照片2', '照片描述2', '照片3', '照片描述3', '餐廳類型1', '餐廳類型2', '餐廳類型3', '網址', '經度', '緯度', '停車資訊', '服務時間']

        return Realfields, merged_list_with_ids


    def get_data_Restaurant_old(self):
        url = 'https://media.taiwan.net.tw/XMLReleaseALL_public/restaurant_C_f.json'
        fields = ['城市', '鄉鎮', '郵遞區號', '地址', 'ID', '名字', '簡述', '電話', '營業時間', '照片1', '照片描述1', '照片2', '照片描述2', '照片3', '照片描述3', '餐廳類型', '網址', '經度', '緯度', '停車資訊']
        #fields = ['city', 'site', 'zipcode', 'add', 'id', 'name', 'description', 'tel', 'time', 'picture1', 'pic_describe1', 'picture2', 'pic_describe2', 'picture3', 'pic_describe3', 'type', 'web', 'Px', 'Py', 'parkinginfo']
        tmpList = []
        data = requests.get(url)
        data.encoding = 'utf-8-sig'
        data_json = data.json()
        info = data_json['XML_Head']['Infos']['Info']
        processed_ids = set()  # To keep track of processed id
        for i in info :
            id = i['Id']
            if id not in processed_ids:
                city = i['Region']
                site = i['Town']
                zipcode = i['Zipcode']
                add = i['Add']
                name = i['Name']
                description = i['Description']
                tel = i['Tel']
                time = i['Opentime']
                picture1 = i['Picture1']
                pic_describe1 =i['Picdescribe1']
                picture2 = i['Picture2']
                pic_describe2 =i['Picdescribe2']
                picture3 = i['Picture3']
                pic_describe3 =i['Picdescribe3']
                class_r = i['Class'] #店家類別:：01.異國料理、02.火烤料理、03.中式美食、04.夜市小吃、05.甜點冰品、06.伴手禮、07.地方特產、08.素食、09.其他
                web = i['Website']
                Px = i['Px']
                Py = i['Py']
                parkinginfo = i['Parkinginfo'] 
                tmpList.append([city, site, zipcode, add, id, name, description, tel, time, picture1, pic_describe1, picture2, pic_describe2, picture3, pic_describe3, class_r, web, Px, Py, parkinginfo])
                processed_ids.add(id)
        return fields,tmpList 
    

def crawler_get_data():
    db = MySQLDatabase(HOST, USER, PASSWORD, DATABASENAME)

    cr_view = Crawler_View()
    fields, data = cr_view.get_data_view()
    db.insertDataInToSql(dataLocation='', table_name='View', fields = fields, insertdata = data, mode='json')

    cr_restaurant = Crawler_Restaurant()
    fields, data = cr_restaurant.get_data_Restaurant()
    db.insertDataInToSql(dataLocation='', table_name='Restaurant', fields = fields, insertdata = data, mode='json')

    cr_activity = Crawler_Activity()
    fields, data = cr_activity.get_data_Activity()
    db.insertDataInToSql(dataLocation='', table_name='Activity', fields = fields, insertdata = data, mode='json')

    cr_get_data_Week = Crawler_Weather()
    fields, data = cr_get_data_Week.get_data_Week()
    db.insertDataInToSql(dataLocation='', table_name='Weather_Week', fields = fields, insertdata = data, mode='json')

    cr_hotel = Crawler_Hotel()
    fields, data = cr_hotel.get_data_Hotel()
    db.insertDataInToSql(dataLocation='', table_name='Hotel', fields = fields, insertdata = data, mode='json')

    cr_get_data_36hours = Crawler_Weather()
    fields, data = cr_get_data_36hours.get_data_36hours()
    db.insertDataInToSql(dataLocation='', table_name='Weather_36hours', fields = fields, insertdata = data, mode='json')

    cr_get_data_2Days = Crawler_Weather()
    fields, data = cr_get_data_2Days.get_data_2Days()
    db.insertDataInToSql(dataLocation='', table_name='Weather_2Days', fields = fields, insertdata = data, mode='json')

def crawler_update_table_data():
    db = MySQLDatabase(HOST, USER, PASSWORD, DATABASENAME)

    downLoadData()
    cr_view = Crawler_View()
    fields, data = cr_view.get_data_view()
    for row in data:
        updateViewDataInToSql(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[0])
        print("View_Data"+str(row)+" done!")
    print("View Data Update Success!")

    cr_restaurant = Crawler_Restaurant()
    fields, data = cr_restaurant.get_data_Restaurant()
    for row in data:
        updateRestaurantDataInToSql(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21],row[0])
        print("Restaurant_Data"+str(row)+" done!")
    print("Restaurant Data Update Success!")

    cr_activity = Crawler_Activity()
    fields, data = cr_activity.get_data_Activity()
    for row in data:
        updateActivityDataInToSql(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[0])
        print("Activity_Data"+str(row)+" done!")
    print("Activity Data Update Success!")

    cr_hotel = Crawler_Hotel()
    fields, data = cr_hotel.get_data_Hotel()
    for row in data:
        updateHotelDataInToSql(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22],row[0])
        print("Hotel_Data"+str(row)+" done!")
    print("Hotel Data Update Success!")
    
def crawler_update_weather_data():
        db = MySQLDatabase(HOST, USER, PASSWORD, DATABASENAME)

        cr_get_data_36hours = Crawler_Weather()
        fields, data = cr_get_data_36hours.get_data_36hours()
        for row in data:
            update36hoursDataInToSql(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[0], row[1], row[2], row[3], row[4])
            print("Weather_36hours_Data"+str(row)+" done!")
        print("Weather 36Hours Data Update Success!")

        cr_get_data_2Days = Crawler_Weather()
        fields, data = cr_get_data_2Days.get_data_2Days()
        for row in data:
            update2daysDataInToSql(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[0], row[1], row[2], row[3], row[4], row[5])
            print("Weather_2days_Data"+str(row)+" done!")
        print("Weather 2Days Data Update Success!")

        cr_get_data_Week = Crawler_Weather()
        fields, data = cr_get_data_Week.get_data_Week()
        for row in data:
            updateweekDataInToSql(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[0], row[1], row[2], row[3], row[4], row[5])
            print("Weather_week_Data"+str(row)+" done!")
        print("Weather Week Data Update Success!")

def fakedata():
    db = MySQLDatabase(HOST, USER, PASSWORD, DATABASENAME)
    db.connect()

    if not db.check_table_exists('manager'):
        db.create_table('manager', ['username', 'userpassword'])
        #insertManagerDataInToSql('root', '1234')
    
    if not db.check_table_exists('trip'):
        db.create_table('trip', ['TravelId', 'DayId', 'ViewId','Type', 'TripId', 'Id', 'date', 'time', 'weather'])
        #insertTripDataInToSql('testTravelID', 'testdayid0', 'testviewid0', 'activity', '0', 'testId_0', 'date1', 'time1', 'sunny')
        #insertTripDataInToSql('testTravelID', 'testdayid1', 'testviewid1', 'activity', '1', 'testId_1', 'date2', 'time2', 'rainy')
    if not db.check_table_exists('travel'):
        db.create_table('travel', ['TravelId', 'TravelName', 'userId'])
        #insertTravelDataInToSql('testTravelID', 'testTravelName', 'testEmail')
    if not db.check_table_exists('user'):
        db.create_table('user', ['email', 'username', 'userpassword'])
        #insertUserDataInToSql('testEmail', 'testName', 'testPassword')

    if not db.check_table_exists('route'):
        db.create_table('route', ['travelId', 'DayId', 'tripStartId', 'routeId', 'routeName'])
        #insertRouteDataInToSql('testTravelID', 'testdayid0', 'testviewid0', 'testRouteID', 'testRouteName')

    if not db.check_table_exists('type'):
        query = """CREATE TABLE `type` (`Type` varchar(255) NOT NULL)"""
        db.execute_query(query)
        typelist = ['activity', 'hotel', 'restaurant', 'view']
        query = "INSERT INTO type (Type) VALUES (%s);"
        for type_ in typelist:
            db.execute_update(query, (type_))

    if not db.check_table_exists('user_record_travel'):
        db.create_table('user_record_travel', ['userId', 'TravelId', 'isLike', 'isStore'])
        #insertRecordTravelDataInToSql('testEmail', 'testTravelID', 0, 0)

    if not db.check_table_exists('user_record_trip'):
        db.create_table('user_record_trip', ['userId', 'TravelId', 'DayId', 'isLike', 'isStore'])
        #insertRecordTripDataInToSql('testEmail', 'testTravelID', 'testdayid0',  0, 0)

    if not db.check_table_exists('user_record_view'):
        db.create_table('user_record_view', ['userId', 'Type', 'ID', 'isLike', 'isStore'])

    if not db.check_table_exists('ids'):
        db.create_table('IDS', ['Type', 'ID', 'name'])
        for type_ in ['activity', 'hotel', 'restaurant', 'view']:
            result = db.execute_query("SELECT `ID`, `名字` FROM " + type_)
            for res in result:
                query = "INSERT INTO IDS (Type, ID, name) VALUES (%s, %s, %s);"
                db.execute_update(query, (type_, res['ID'], res['名字']))
    db.disconnect()

def get_type_classes():
    db = MySQLDatabase(HOST, USER, PASSWORD, DATABASENAME)
    db.connect()
    view_classes = [
                    { "class": "1", "name": "文化類" },
                    { "class": "2", "name": "生態類" },
                    { "class": "3", "name": "文化遺產類" },
                    { "class": "4", "name": "宗教廟宇類" },
                    { "class": "5", "name": "藝術類" },
                    { "class": "6", "name": "商圈商店類" },
                    { "class": "7", "name": "國家公園類" },
                    { "class": "8", "name": "國家風景區類" },
                    { "class": "9", "name": "休閒農業類" },
                    { "class": "10", "name": "溫泉類" },
                    { "class": "11", "name": "自然風景類" },
                    { "class": "12", "name": "遊憩類" },
                    { "class": "13", "name": "體育健身類" },
                    { "class": "14", "name": "觀光工廠類" },
                    { "class": "15", "name": "都會公園類" },
                    { "class": "16", "name": "森林遊樂區類" },
                    { "class": "17", "name": "平地森林園區類" },
                    { "class": "18", "name": "國家自然公園類" },
                    { "class": "19", "name": "公園綠地類" },
                    { "class": "20", "name": "觀光遊樂業類" },
                    { "class": "21", "name": "原住民文化類" },
                    { "class": "22", "name": "客家文化類" },
                    { "class": "23", "name": "交通場站類" },
                    { "class": "24", "name": "水域環境類" },
                    { "class": "25", "name": "藝文場館類" },
                    { "class": "26", "name": "生態場館類" },
                    { "class": "27", "name": "娛樂場館類" },
                    { "class": "254", "name": "其他" },
                ]
    restaurant_classes = [
                        { "class": "1", "name": "台灣小吃/台菜" },
                        { "class": "2", "name": "中式料理 — 中餐八大菜系" },
                        { "class": "3", "name": "港式料理" },
                        { "class": "4", "name": "日式料理" },
                        { "class": "5", "name": "韓式料理" },
                        { "class": "96", "name": "南亞料理 — 印度、馬來西亞等料理" },
                        { "class": "97", "name": "東南亞料理 — 泰國、馬來西亞等料理" },
                        { "class": "98", "name": "美式/歐式料理 — 義、法、德、西班牙、俄羅斯等料理" },
                        { "class": "99", "name": "其他異國料理" },
                        { "class": "100", "name": "夜市小吃" },
                        { "class": "101", "name": "甜點冰品" },
                        { "class": "102", "name": "麵包糕點" },
                        { "class": "103", "name": "非酒精飲品 — 咖啡、茶等" },
                        { "class": "104", "name": "酒類飲品 — 雞尾酒、葡萄酒、烈酒等" },
                        { "class": "105", "name": "燒烤/鐵板燒" },
                        { "class": "106", "name": "火鍋" },
                        { "class": "107", "name": "海鮮" },
                        { "class": "108", "name": "牛排" },
                        { "class": "109", "name": "速食" },
                        { "class": "110", "name": "連鎖餐飲" },
                        { "class": "111", "name": "吃到飽" },
                        { "class": "112", "name": "便當/自助餐" },
                        { "class": "113", "name": "牛肉麵" },
                        { "class": "114", "name": "粥品" },
                        { "class": "115", "name": "地方特產" },
                        { "class": "116", "name": "伴手禮/禮盒" },
                        { "class": "200", "name": "純素飲食" },
                        { "class": "201", "name": "素食飲食" },
                        { "class": "202", "name": "清真飲食" },
                        { "class": "203", "name": "無麩質飲食" },
                        { "class": "204", "name": "健康飲食 — 低鹽、低熱量等" },
                        { "class": "254", "name": "其他" },
                        ]

    activity_classes = [
                        { "class": "1", "name": "節慶活動" },
                        { "class": "2", "name": "藝文活動" },
                        { "class": "3", "name": "年度活動" },
                        { "class": "4", "name": "遊憩活動" },
                        { "class": "5", "name": "地方社區型活動" },
                        { "class": "9", "name": "其他活動" },
                        ]

    hotel_classes = [
                    { "class": "1", "name": "國際觀光旅館" },
                    { "class": "2", "name": "一般觀光旅館" },
                    { "class": "3", "name": "一般旅館" },
                    { "class": "4", "name": "民宿" },
                    { "class": "5", "name": "露營區" },
                    { "class": "9", "name": "其他" },
                    ]
    
    arrs = [view_classes, restaurant_classes, activity_classes, hotel_classes]
    arrss = ['view', 'restaurant', 'activity', 'hotel']
    if not db.check_table_exists('type_classes'):
        db.create_table('type_classes', ['type', 'class', 'name'])
    for idx, arr in enumerate(arrs):
        for val in (arr):
            query = """INSERT INTO `type_classes` (`type`, `class`, `name`) VALUES (%s, %s, %s)"""
            db.execute_query(query, (arrss[idx], val["class"], val['name'],))
    db.disconnect()
def SettingPRIMARY_KEY():
    db = MySQLDatabase(HOST, USER, PASSWORD, DATABASENAME)
    db.connect()
    primaryQuery = [
                    #manager
                    """ALTER TABLE `manager` ADD PRIMARY KEY (`username`);""",
                    #user
                    """ALTER TABLE `user` ADD PRIMARY KEY (`email`);""",
                    #travel
                    """ALTER TABLE `travel` ADD PRIMARY KEY (`TravelId`);""",
                    #trip
                    """ALTER TABLE `trip` ADD PRIMARY KEY (`TravelId`, `DayId`, `ViewId`);""",

                    #route
                    """ALTER TABLE `route` ADD PRIMARY KEY (`travelId`, `DayId`, `tripStartId`, `routeId`);""",

                    #user_record_travel
                    """ALTER TABLE `user_record_travel` ADD PRIMARY KEY (`userId`, `TravelId`);""",

                    #user_record_trip
                    """ALTER TABLE `user_record_trip` ADD PRIMARY KEY (`userId`, `TravelId`, `DayId`);""",

                    #user_record_view
                    """ALTER TABLE `user_record_view` ADD PRIMARY KEY ( `userId`,`Type`, `ID`);""",

                    #type
                    """ALTER TABLE `type` ADD PRIMARY KEY (`Type`);""",

                    #city
                    """ALTER TABLE `city` ADD PRIMARY KEY (`city_id`);""",

                    #site
                    """ALTER TABLE `site` ADD PRIMARY KEY (`city_id`, `site_id`);""",

                    #view
                    """ALTER TABLE `view` ADD PRIMARY KEY (`ID`);""",

                    #weather
                    """ALTER TABLE `weather_week` ADD PRIMARY KEY (`城市`, `鄉鎮`, `開始日期`, `開始時間`, `結束日期`, `結束時間`);""",

                    #weather_2Days
                    """ALTER TABLE `weather_2days` ADD PRIMARY KEY (`城市`, `鄉鎮`, `開始日期`, `開始時間`, `結束日期`, `結束時間`);""",

                    #weather_36hours
                    """ALTER TABLE `weather_36hours` ADD PRIMARY KEY (`城市`, `開始日期`, `開始時間`, `結束日期`, `結束時間`);""",

                    #activity
                    """ALTER TABLE `activity` ADD PRIMARY KEY (`ID`);""",

                    #hotel
                    """ALTER TABLE `hotel` ADD PRIMARY KEY (`ID`);""",

                    #restaurant
                    """ALTER TABLE `restaurant` ADD PRIMARY KEY (`ID`);""",

                    #ids
                    """ALTER TABLE `ids` ADD PRIMARY KEY (`Type`, `ID`);""",
                    # type_classes
                    """ALTER TABLE `type_classes` ADD PRIMARY KEY (`type`, `class`);"""
                    ]
    for query in primaryQuery:
        print(query)
        db.execute_query(query)
    db.disconnect()

def SettingFOREIGN_KEY():
    db = MySQLDatabase(HOST, USER, PASSWORD, DATABASENAME)
    db.connect()
    foreignQuery = [#city manager user and type don't need to do
        
                    #site
                    """ALTER TABLE `site` ADD FOREIGN KEY (`city_id`) REFERENCES `city` (`city_id`) ON DELETE CASCADE ON UPDATE CASCADE """,
                    
                    #travel
                    'ALTER TABLE `travel` ADD FOREIGN KEY (`userId`) REFERENCES `user`(`email`) ON DELETE CASCADE ON UPDATE CASCADE;',
                    #trip
                    'ALTER TABLE `trip` ADD FOREIGN KEY (`TravelId`) REFERENCES `travel`(`TravelId`) ON DELETE CASCADE ON UPDATE CASCADE;',
                    'ALTER TABLE `trip` ADD FOREIGN KEY (`Type`) REFERENCES `type`(`Type`) ON DELETE CASCADE ON UPDATE CASCADE;',
                    #route
                    """ALTER TABLE `route` ADD FOREIGN KEY (`travelId`, `DayId`, `tripStartId`) REFERENCES `trip`(`TravelId`, `DayId`, `ViewId`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    #user_record_travel
                    """ALTER TABLE `user_record_travel` ADD FOREIGN KEY (`userId`) REFERENCES `user`(`email`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    """ALTER TABLE `user_record_travel` ADD FOREIGN KEY (`TravelId`) REFERENCES `travel`(`TravelId`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    #user_record_trip
                    """ALTER TABLE `user_record_trip` ADD FOREIGN KEY  (`userId`) REFERENCES `user`(`email`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    """ALTER TABLE `user_record_trip` ADD FOREIGN KEY (`TravelId`, `DayId`) REFERENCES `trip`(`TravelId`, `DayId`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    #user_record_view
                    """ALTER TABLE `user_record_view` ADD FOREIGN KEY (`userId`) REFERENCES `user`(`email`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    """ALTER TABLE `user_record_view` ADD FOREIGN KEY (`Type`, `ID`) REFERENCES `ids`(`Type`, `ID`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    #weather
                    """ALTER TABLE `weather_36hours` ADD FOREIGN KEY (`城市`) REFERENCES `city`(city_id) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    """ALTER TABLE `weather_week` ADD FOREIGN KEY (`城市`, `鄉鎮`) REFERENCES `site`(`city_id`, `site_id`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    """ALTER TABLE `weather_2days` ADD FOREIGN KEY (`城市`, `鄉鎮`) REFERENCES `site`(`city_id`, `site_id`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    #activity
                    """ALTER TABLE `activity` ADD FOREIGN KEY (`城市`, `鄉鎮`) REFERENCES `site`(`city_id`, `site_id`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    #hotel
                    """ALTER TABLE `hotel` ADD FOREIGN KEY (`城市`, `鄉鎮`) REFERENCES `site`(`city_id`, `site_id`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    #restaurant
                    """ALTER TABLE `restaurant` ADD FOREIGN KEY (`城市`, `鄉鎮`) REFERENCES `site`(`city_id`, `site_id`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    #view
                    """ALTER TABLE `view` ADD FOREIGN KEY (`城市`, `鄉鎮`) REFERENCES `site`(`city_id`, `site_id`) ON DELETE CASCADE ON UPDATE CASCADE;""",
                    #type_classes
                    """ALTER TABLE `type_classes` ADD FOREIGN KEY (`type`) REFERENCES `type`(`Type`) ON DELETE CASCADE ON UPDATE CASCADE;"""
                    ]

    for fk in foreignQuery:
        print(fk)
        db.execute_query(fk)
    db.connection.commit()
    db.disconnect()

def get_city_site():
    Authorization = 'Authorization=CWA-B7166A49-DAA0-489F-913D-F6593E39785D'
    city = []
    citysite = []

    for idx in range(23):
        file_name = 'F-D0047-'
        file_name += '00' + str(idx*4+3) if (idx*4+3) < 10 else '0' + str(idx*4+3)
        url = f'https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/{file_name}?{Authorization}&downloadType=WEB&format=JSON'
        
        data = requests.get(url)
        data_json = data.json()
        
        locations = data_json['cwaopendata']['dataset']['locations']['location']
        location_name = data_json['cwaopendata']['dataset']['locations']['locationsName']

        if location_name == '台灣':
            continue

        city.append(location_name)
        for location in locations:
            site = location['locationName']
            citysite.append((location_name, site))

        city = list(set(city))
        citysite = list(set(citysite))
    return city, citysite

def initialize():
    try:
        print('initializing...')
        print('generating city and site...')
        initcitysite()
    except:
        pass
    try:
        print('downloading data...')
        downLoadData()
    except:
        pass
    try:
        print('crawling all data...')
        crawler_get_data()
    except:
        pass
    print('generating necessary table...')
    fakedata()
    try:
        print('get get type_lasses...')
        get_type_classes()
    except:
        pass
    try:
        print('setting primary key...')
        SettingPRIMARY_KEY()
        print('setting foreign key...')
        SettingFOREIGN_KEY()
    except:
        pass
    print('done!')
    

def update():
    crawler_update_weather_data()
    crawler_update_table_data()

def initcitysite():
    db = MySQLDatabase(HOST, USER, PASSWORD, DATABASENAME)

    #create and insert city data
    db.execute_update(city_query_create)
    city_data, citysite_data = get_city_site()

    for data in city_data:
        db.execute_update(city_query_insert, data)

    #create and insert site data
    db.execute_update(site_query_create)
    for data in citysite_data:
        db.execute_update(site_query_insert, data)

    db.disconnect()

def return_date_time(original_string):
    original_dt = datetime.fromisoformat(original_string)
    offset_timedelta = timedelta(hours=8)
    combined_dt = datetime.combine(original_dt.date(), original_dt.time()) + offset_timedelta

    return combined_dt.date().strftime("%Y-%m-%d"), combined_dt.time().strftime("%H:%M:%S")



def downLoadData():
    url_base = 'https://media.taiwan.net.tw/XMLReleaseAll_public/v2.0/Zh_tw/'
    data = ['Attraction-json.zip', 'Event-json.zip', 'Restaurant-json.zip', 'Hotel-json.zip']
    dirs = ['./opensource/attraction', './opensource/event', './opensource/restaurant', './opensource/hotel']
    local_zip_path = ['./opensource/attraction/Attraction-json.zip', './opensource/event/Event-json.zip', './opensource/restaurant/Restaurant-json.zip', './opensource/hotel/Hotel-json.zip']

    for dir_name in dirs:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

    for idx, small_url in enumerate(data):
        with urllib.request.urlopen(url_base + small_url) as response:
            with open(local_zip_path[idx], 'wb') as out_file:
                out_file.write(response.read())

        print(f"Downloaded file saved as {local_zip_path[idx]}")

        with zipfile.ZipFile(local_zip_path[idx], 'r') as zip_ref:
            zip_ref.extractall(dirs[idx])

        print("ZIP file extracted successfully")

        extracted_files = os.listdir('./opensource')
        print("Extracted files:", extracted_files)


if __name__ == '__main__':
    initialize() #註解拿掉可以取得所有資料
    #update()
    pass
