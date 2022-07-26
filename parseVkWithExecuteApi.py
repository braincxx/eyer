import asyncio
import os
import sys
from weakref import proxy
from aiohttp import ClientSession
import json
import nest_asyncio
import random
from datetime import datetime
from collections import deque
import urllib.request
import time
import pyodbc
nest_asyncio.apply()
import requests

import psycopg2
"""Рабочий коннект"""
conn = psycopg2.connect(dbname='postgres', user='postgres', password='postgrespw', host='192.168.0.103', port=49158)
#conn = psycopg2.connect(dbname='research', user='user', password='1234', host='10.14.0.69', port=5555)
"""
with open("proxy_list.txt", "r") as f:
    proxyList = [i.strip().split() for i in f.readlines()]
    """
    
"""
def lProxy():
    prx = random.choice(proxyList)
    return f"{prx[2]}://{prx[0]}:{prx[1]}"
"""

start_full = time.time()
list_data=[]

idSessionParseFriends = 0
idSessionParsePhoto = 0
q = deque()

def initSession():
    global idSessionParseFriends
    global idSessionParsePhoto
    with conn.cursor() as cursor:
        conn.autocommit = True
        try:
            cursor.execute("""INSERT INTO public."initGetUserSession" ("initDatetime")
	                            VALUES ('{}');""".format(datetime.now()))

            cursor.execute("""select "idSession" from  public."initGetUserSession" order by "idSession" DESC limit 1""")
            idSessionParseFriends = cursor.fetchone()[0]
        except Exception as e:
            print("database", e)
    with conn.cursor() as cursor:
        conn.autocommit = True
        try:
            cursor.execute("""INSERT INTO public."initGetPhotoSession"(
                                "initDatetime")
                                VALUES ('{}');""".format(datetime.now()))

            cursor.execute("""SELECT "idSessionPhoto"
	FROM public."initGetPhotoSession" order by "idSessionPhoto" DESC limit 1""")
            idSessionParsePhoto = cursor.fetchone()[0]
        except Exception as e:
            print("database", e)
initSession()

def getPhotoUser(item, write2DB=False):
    global idSessionParsePhoto
    global idOwnerPhoto

    try:
        #s = api.photos.getAll(owner_id=owner_id) #one item user
        s = item
        z = item['items']
    except Exception as e:
       # print("add to queue {}".format(owner_id))
        print(e)
       # idOwnerPhoto.append(owner_id)
        return
    
    for itemPhoto in s['items']:
        id_photo = itemPhoto['id']
        owner_id = itemPhoto['owner_id']

        maxSize = itemPhoto['sizes'][0]['height']
        maxType = itemPhoto['sizes'][0]['type']
        for itemSize in itemPhoto['sizes']:
            if itemSize["height"] > maxSize:
                maxSize = itemSize["height"]
                maxType = itemSize['type']
        try:
            url = [i['url'] for i in itemPhoto['sizes'] if i['type'] == maxType][0]
        except Exception as e :
            print(e)
        
        ###WRITE USER IN TABLE USERS
        with conn.cursor() as cursor:
            conn.autocommit = True
            try:
                cursor.execute("""INSERT INTO public."Users"(
                                "idUser", nickname, "idSession")
                                VALUES ('%s', '%s', %d);""" % (str(owner_id), str(owner_id), idSessionParseFriends))
            except Exception as e:
                print("database", e)
                ###FOR REWRITE
                cursor.execute("""delete from public."Users" where "idUser"='%s'""".format(str(id)))
                cursor.execute("""INSERT INTO public."Users"(
                                "idUser", nickname, "idSession")
                                VALUES ('%s', '%s', %d);""" % (str(owner_id), str(owner_id), idSessionParseFriends))
                with open("log.txt", "a") as f:
                    f.write("DONT ADD TO id %s \n" % id)

        if (write2DB):
            with conn.cursor() as cursor:
                conn.autocommit = True
                try:
                    cursor.execute("""INSERT INTO public."Photos"(
            "idPhoto", "idUser", link2photo, "idPhotoSession")
            VALUES (%d, '%s', '%s', %d);""" % (int(id_photo), str(owner_id), url, idSessionParsePhoto))
                except Exception as e:
                    cursor.execute("""update public."Photos" set "idPhotoSession"=%d where "idPhoto"=%d""" % (idSessionParsePhoto, int(id_photo)))

                    print("database", e)

                    

                    with open("log.txt", "a") as f:
                        f.write("DONT ADD TO id %d \n" % id_photo)

with open ("tokens.txt", "r") as f:
    list_token = [i.strip() for i in f.readlines()]
    random.shuffle(list_token)

async def bound_fetch_zero(sem, id, session):
    async with sem:
        await fetch_zero(id,session)
 

#print(list_token)
async def fetch_zero(id, session):
    url = build_url(id)
    try:#proxy="http://proxy.com")
        start = time.time()
        #prx = ["188.243.219.133", 61031, "CeNdyK", "um5YZaS3pEer", 1645401600]
        #["91.188.228.37", 57295, "dtiBPkKp", "iX4U87RK", 1646092800]
        #, proxy="https://dtiBPkKp:iX4U87RK@91.188.228.37:57295"
        async with session.get(url) as response:

                # Считываем json
            
            resp = await response.text()
            #print(resp)
            
            js = json.loads(resp)
            if "error" in js:
                raise Exception(js['error']['error_msg'])
            elif js["execute_errors"][0]["error_msg"] == "Rate limit reached":
                raise Exception("Rate limit reached")
            else:
                print(time.time() - start, id)
            list_photos = [x for x in js['response'] if x != False] #[0] - from one user

            # Проверяем если город=1(Москва) тогда добавляем в лист
            for itemOneUser in list_photos:
                
                getPhotoUser(itemOneUser, True)
                """
                try:
                    if it[0]['city']['id']==1:
                            list_data.append(it[0]['id'])
                except Exception:
                    pass
                """
    except Exception as ex:
        print(ex, time.time() - start, id)
        #print(ex)
        #print(f'Error: {js}')
 
#  Генерация url к апи вк, 25 запросов в одном
def build_url(id):
    api = f"""API.photos.getAll({{'owner_id':{id + 1},'count':200}})"""
   # api_2 = f"""API.photos.getAll({{'owner_id':174528152,'count':200}})"""
    for i in range(2, 26):
        api += f""",API.photos.getAll({{'owner_id':{id + i},'count':200}})"""
    
    #url = 'https://api.vk.com/method/execute?access_token={}&v=5.101&code=return%20[{}];'.format(
#        list_token[id%len(list_token)], api)
    #WORK url = f"""https://api.vk.com/method/execute?access_token={random.choice(list_token)}&v=5.101&code=return%20[API.photos.getAll({{'owner_id':174528152,'count':200}}), API.photos.getAll({{'owner_id':174528153,'count':200}})];""" 
    url = f"""https://api.vk.com/method/execute?access_token={list_token[id % len(list_token)]}&v=5.101&code=return%20[{api}];"""
     #   list_token[id%len(list_token)])#, api)
     #
    print(url)
    return url
"""
def build_url(id):
    api = 'API.users.get({{\'user_ids\':{},\'fields\':\'city\'}})'.format(
        id * 25 + 1)
    for i in range(2, 26):
        api += ',API.users.get({{\'user_ids\':{},\'fields\':\'city\'}})'.format(
            id * 25 + i)
    url = 'https://api.vk.com/method/execute?access_token={}&v=5.101&code=return%20[{}];'.format(
        list_token[id%len(list_token)], api)
    return url
 """
 #{'code': 'var params = {"owner...ls != 99};', 'v': '5.92', 'access_token': '7be168a46cb69540b971...fcf8f20421'}
 #'var params = {"owner_id":174528152,"count":200},calls = 0,items = [],count = null,offset = 0,ri;while(calls < 25) {calls = calls + 1;params.offset = offset * 1;var response = API.photos.getAll(params),new_count = response.count,count_diff = (count == null ? 0 : new_count - count);if (!response) {return {"_error": 1};}if (count_diff < 0) {offset = offset + count_diff;} else {ri = response.items;items = items + ri.slice(count_diff);offset = offset + params.count + count_diff;if (ri.length < params.count) {calls = 99;}}count = new_count;if (count != null && offset >= count) {calls = 99;}};return {count: count,items: items,offset: offset,more: calls != 99};'

#batch size(count threads)
#id - from current to batch size * 10
countTokens = 100 #формула: count_token * 2 + 1
id_start = 610000

async def run_zero(id):
    tasks = []
    sem = asyncio.Semaphore(1000)
 #
    async with ClientSession() as session:
 				
      	#  Значение 3200 зависит от вашего числа токенов
        #for id_step in range(id, id + 10 * countTokens, countTokens):
        #id_step = id
        for id_target in range(id, id + countTokens):
            task = asyncio.ensure_future(bound_fetch_zero(sem, id_target, session))
            tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses
            
       # responses = asyncio.gather(*tasks)
       # await responses
        del responses
        await session.close()
#for i in range(217723911, 217723911+1):

for id_step in range(id_start, id_start + 10 * countTokens, countTokens):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_zero(id_step))
#217723
# Запускаем  сборщик
"""
async with ClientSession() as session:
 				
      	#  Значение 3200 зависит от вашего числа токенов 
    for id in range((id - 1) * 3200, id * 3200):
        task = asyncio.ensure_future(bound_fetch_zero(sem,id, session))
        tasks.append(task)

    responses = asyncio.gather(*tasks)
    await responses
    del responses
    await session.close()

for i in range(0,1):
    for id in range(i*500+1,(i+1)*500+1):
        print(id)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_zero(id))"""

"""
  # Сохраняем айдишники в файл на гугл диске и очищаем лист
  with open(f'/content/gdrive/My Drive/data_main{i}.txt', 'w') as f:
            for item in list_data:
              f.write(f'{item}\n')"""

#print(list_data)
print(f"FULL TIME {time.time() - start_full}")
list_data.clear()
#example for Dima
#test from dimasik

class VkApi():
    id = 6222824
    def __init__(self):

        self._token = self.init_token()


    def init_token(self):
        with open("tokens.txt", "r") as f:
            tokens_list = [i.strip() for i in f.readlines()]

            return random.choice(tokens_list)

    def build_url(self, id:str) -> str:

        api = f"""API.friends.get({{'user_id':{id},'count':200}})"""


        url = f"""https://api.vk.com/method/execute?access_token={self._token}&v=5.101&code=return%20[{api}];"""

        return url

    def build_url(self, idList: list) -> str:
        api = f"""API.friends.get({{'user_id':{id},'count':200}})"""

        for i in range(2, 26):
            api += f""",API.friends.get({{'user_id':{id + i},'count':200}})"""

        url = f"""https://api.vk.com/method/execute?access_token={self._token}&v=5.101&code=return%20[{api}];"""
        return url

    def buildUrlInterface(self, apiFuncParametrsList: list) -> str:

        api = f"""{apiFuncParametrsList[0]['function']}({{ {", ".join(["'" + [(k, p) for (k, p) in param.items()][0][0] + "':" + str([(k, p) for (k, p) in param.items()][0][1]) for param in apiFuncParametrsList[0]['parameters']]) }  }} )"""
        for i in range(1, len(apiFuncParametrsList)):
            api += f""",{apiFuncParametrsList[i]['function']}({{ {", ".join(["'" + [(k, p) for (k, p) in param.items()][0][0] + "':" + str([(k, p) for (k, p) in param.items()][0][1]) for param in apiFuncParametrsList[i]['parameters']]) } }})"""

        url = f"""https://api.vk.com/method/execute?access_token={list_token[id % len(list_token)]}&v=5.101&code=return%20[{api}];"""

        return url

    def buildUrlInterfaceForPhoto(self, apiFuncParametrs: int) -> str:

        api = f"""{apiFuncParametrs['function']}({{ {", ".join(["'" + [(k, p) for (k, p) in param.items()][0][0] + "':" + str([(k, p) for (k, p) in param.items()][0][1]) for param in apiFuncParametrs['parameters']]) }  }} )"""
        #for i in range(1, len(apiFuncParametrs)):
            #api += f""",{apiFuncParametrs[i]['function']}({{ {", ".join(["'" + [(k, p) for (k, p) in param.items()][0][0] + "':" + str([(k, p) for (k, p) in param.items()][0][1]) for param in apiFuncParametrs[i]['parameters']]) } }})"""

        url = f"""https://api.vk.com/method/execute?access_token={list_token[id % len(list_token)]}&v=5.101&code=return%20[{api}];"""

        return url

    def getFriend(self, id: int) -> list:

        apiFuncParametrsList = [
            {
                'function': 'API.friends.get',
                'parameters':
                    [
                    {'user_id': id},
                    {'count': 200}
                ]
            }
        ]
        u = self.buildUrlInterface(apiFuncParametrsList)

        url = self.build_url(id)
        #url = self.build_url(parametrsApiFunc)
        res = requests.get(url).json()
        return res['response'][0]['items']

    def getFriends(self, idList: list) -> list:

        apiFuncParametrsList = []

        for id in idList:
            apiFuncParametrsList.append(
                {
                    'function': 'API.friends.get',
                    'parameters': [
                        {'user_id': id},
                        {'count': 200}
                    ]
                }
            )
        u = self.buildUrlInterface(apiFuncParametrsList)

        url = self.build_url(id)
        #url = self.build_url(parametrsApiFunc)
        res = requests.get(url).json()
        return res['response'][0]['items']


    async def fetchUrlData(self, session_for_async, url):
        try:
            async with session_for_async.get(url, timeout=60) as response:
                resp = await response.json()
                #resp = await response.read()
                for i in range(len(resp['response'][0]["items"])):
                    photo_url = str(resp['response'][0]["items"][i]["sizes"][len(resp['response'][0]["items"][i]["sizes"]) - 1]["url"])
                    print(photo_url) # link for download!
        except Exception as e:
            print(e)
        # else:  впринципе не нужно
        #     return resp
        # return

    async def fetchAsync(self, apiParam):
        api = f"""{apiParam['function']}({{ {", ".join(["'" + [(k, p) for (k, p) in param.items()][0][0] + "':" + str([(k, p) for (k, p) in param.items()][0][1]) for param in apiParam['parameters']])}  }} )"""

        url = f"""https://api.vk.com/method/execute?access_token={list_token[id % len(list_token)]}&v=5.101&code=return%20[{api}];"""
        tasks = []
        async with ClientSession() as session_for_async:
            task = asyncio.ensure_future(self.fetchUrlData(session_for_async, url))
            tasks.append(task)
            responses = await asyncio.gather(*tasks)
        return responses



    def getPhotos(self, idList: list) -> list:

        # parametrsApiFuncList = []
        for id in idList:
            #count = 0
            parametrsApiFuncList = {
                'function': 'API.photos.getAll',
                'parameters': [
                    {'owner_id': id},
                    {'count': 200}
                ]
            }

            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(self.fetchAsync(parametrsApiFuncList))
            loop.run_until_complete(future)
            responses = future.result()

            # newpath = os.path.join(sys.path[0], str(id))
            # if not os.path.exists(newpath):
            #     os.makedirs(newpath)

            # u = self.buildUrlInterfaceForPhoto(parametrsApiFuncList)
            # result_foto = requests.get(u).json()
            # print(result_foto)

            # for i in range(len(result_foto['response'][0]["items"])):
            #     count += 1
            #     # берём ссылку на максимальный размер фотографии
            #     photo_url = str(result_foto['response'][0]["items"][i]["sizes"][
            #                         len(result_foto['response'][0]["items"][i]["sizes"]) - 1]["url"])
            #     print(count)
            #     # скачиваем фото в папку с ID пользователя
            #     # urllib.urlretrieve(photo_url, newpath + '/' + str(response["items"][i]['id']) + '.jpg')
            #     urllib.request.urlretrieve(photo_url,
            #                                newpath + '/' + str(result_foto['response'][0]["items"][i]['id']) + '.jpg')