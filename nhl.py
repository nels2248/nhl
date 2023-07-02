#THIS REPORT LOOPS THROUGH THREE CALLS TO DATA END POINTS ON THE STATSAPI.WEB.NHL.COM
#1. TEAMS
#2. ROSTER
#3. PEOPLE

#IT HAS TWO LOOPS.  
#FIRST LOOP GOES THROUGH THE TEAMS AND ADDS EACH TEAMS ROSTER TO A ROSTER DATAFRAME
#SECOND LOOP GOES THROUGH EACH ROSTER PLAYER AND PULLS MORE PEOPLE DATA

#THIS EXPORTS TO A CSV THAT I USE TO LOAD INTO ARCGIS ONLINE TO SHOW ON A MAP
#NEXT STEP IS TO FIND A TOOL THAT AUTOMATES THIS TO REFRESH AT LEAST NIGHTLY
#https://www.arcgis.com/apps/mapviewer/index.html?webmap=f665674066d54fae9f295efa7099b401

#IMPORT STATEMENTS
import requests
import json
import pandas as pd

#THIS CALLS ANOTHER gmapskey.py file where I'm storing the google maps key that is used later down the code.  
import gmapskey

#GLOBAL VARIABLES
base_url = "https://maps.googleapis.com/maps/api/geocode/json?"

#FUNCTION THAT PASSES AN ADDRESS TO RETURN A LAT, LONG FROM GOOGLE MAPS API.  
#NOTE IT PASSES THE KEY FROM THE gmapskey file THAT IS NOT SAVED HERE BUT IN A SEPERATE FILE.  
def getLocation(address):
    params={
         "key": gmapskey.gmapskey,
         "address": address
    }
    response = requests.get(base_url, params = params).json()
    print(response.keys())
    print(address)
    if response ["status"] == "OK":
        geometry = response["results"][0]["geometry"]
        longitude = geometry["location"]["lng"]
        latitude = geometry["location"]["lat"]
        returnvalue = f"{longitude}" + "," + f"{latitude }"
    else:
         returnvalue = "0,0"
    
    return returnvalue

#CREATE TEAMS DATA FRAME
response_teams = requests.get('https://statsapi.web.nhl.com/api/v1/teams').text
response_info_teams = json.loads(response_teams)
df_teams = pd.json_normalize(response_info_teams['teams'])


#LOOP # 1
#LOOP THROUGH TEAMS DATA FRAME AND ADD EACH TEAMS ROSTER TO ANOTHER DATA FRAME
for index, row in df_teams.iterrows():
    print(row)
    response_roster = requests.get('https://statsapi.web.nhl.com/api/v1/teams/' + str(row['id']) + '/roster').text
    response_info_roster = json.loads(response_roster)
    if index == 0:
        df_roster = pd.json_normalize(response_info_roster['roster'])
    else:
        df_roster_append = pd.json_normalize(response_info_roster['roster'])
        df_roster = pd.concat([df_roster, df_roster_append], axis=0)
#LOOP #2
#LOOP THROUGH EACH PLAYER ON EACH ROSTER AND CALL THE PEOPLE END POINT
for i, (index, row) in enumerate(df_roster.iterrows()):
    print(row)
    response_people = requests.get('https://statsapi.web.nhl.com/api/v1/people/' + str(row['person.id'])).text
    response_info_people = json.loads(response_people)
    if i == 0:
        df_people = pd.json_normalize(response_info_people['people'])
    else:
        df_people_append = pd.json_normalize(response_info_people['people'])
        df_people = pd.concat([df_people, df_people_append], axis=0)
        
#ADD BIRTH CITY, STATE/PROVINCE, COUNTRY TOGETHER
df_people['BIRTH_CITY_STATE_COUNTRY'] = df_people['birthCity'] + " " + df_people['birthStateProvince'].fillna('') + " " + df_people['birthCountry'] 

#CALL THE getLocation() FUNCTION TO POPULATE THE LAT, LONG INTO ANOTHER COLUMN
df_people["latlong"] = df_people['BIRTH_CITY_STATE_COUNTRY'].map(lambda a: getLocation(a))

#SPLIT OUT THE LAT, LONG AGAIN
df_people[['lat', 'long']] = df_people['latlong'].str.split(',', expand=True)

#CONVERT LAT AND LONG TO FLOATS
#THIS MAY BE ANOTHER AREA THAT COULD BE MORE EFFICIENT.  
df_people['lat'] = df_people['lat'].astype(float)
df_people['long'] = df_people['long'].astype(float)

  
#SAVE THE DATA FRAME TO A CSV FILE        
df_people.to_csv('nhl_people.csv')