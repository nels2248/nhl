#THIS REPORT LOOPS THROUGH THREE CALLS TO DATA END POINTS ON THE STATSAPI.WEB.NHL.COM
#1. TEAMS
#2. ROSTER
#3. PEOPLE

#IT HAS TWO LOOPS.  
#FIRST LOOP GOES THROUGH THE TEAMS AND ADDS EACH TEAMS ROSTER TO A ROSTER DATAFRAME
#SECOND LOOP GOES THROUGH EACH ROSTER PLAYER AND PULLS MORE PEOPLE DATA

#NEXT STEP FOR THIS IS TO FIGUE OUT WHAT TO DO WITH IT.  
#RIGHT NOW IT JUST PUTS THE DATA INTO A CSV FILE

#IMPORT STATEMENTS
import requests
import json
import pandas as pd

#CREATE TEAMS DATA FRAME
response_teams = requests.get('https://statsapi.web.nhl.com/api/v1/teams').text
response_info_teams = json.loads(response_teams)
df_teams = pd.json_normalize(response_info_teams['teams'])

#LOOP # 1
#LOOP THROUGH TEAMS DATA FRAME AND ADD EACH TEAMS ROSTER TO ANOTHER DATA FRAME
for index, row in df_teams.iterrows():
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
    response_people = requests.get('https://statsapi.web.nhl.com/api/v1/people/' + str(row['person.id'])).text
    response_info_people = json.loads(response_people)
    if i == 0:
        df_people = pd.json_normalize(response_info_people['people'])
    else:
        df_people_append = pd.json_normalize(response_info_people['people'])
        df_people = pd.concat([df_people, df_people_append], axis=0)
  
#SAVE THE DATA FRAME TO A CSV FILE        
df_people.to_csv('nhl_people.csv')