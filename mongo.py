# -*- coding: utf-8 -*-
"""

@author: Balaji, Aishwarya Somesula
"""

from pymongo import MongoClient
from lxml import etree
import csv

client = MongoClient('localhost:27017')
db = client.database1


with open('world-cup-teams-text.csv', encoding='utf-8') as f:
    next(f)
    csv_f = csv.reader(f)
    for row in csv_f:
        db.team.insert_one({"Team_ID" : row[0] ,"Team_Name" : row[1],"Continent":row[2],"League":row[3],"Population":row[4]})
  

with open('players-text.csv', encoding='latin1') as f: 
    next(f)
    csv_f1 = csv.reader(f)
    for row in csv_f1:
        db.player.insert_one({"Team_Name" : row[0] ,"Team_ID" : row[1],"PNo":row[2],
                              "Position":row[3],"PName":row[4],"Birth_Date":row[5],"Shirt_Name":row[6],"Club":row[7],"Height":row[8],"Weight":row[9]})

with open('world-cup-schedule-results-text.csv', encoding='utf-8') as f: 
    next(f)
    csv_f2 = csv.reader(f)  
    for row in csv_f2:
        db.game.insert_one({"GameID":row[0],"MatchType":row[1],"MatchDate":row[2],"SID":row[3],"TeamID1":row[4],"TeamID2":row[5],"Team1_score":row[6],"Team2_score":row[7]})
  

with open('world-cup-starting-lineups-text.csv', encoding='utf-8') as f: 
    next(f)
    csv_f3 = csv.reader(f)
    for row in csv_f3:
        db.starting_lineups.insert_one({"GameID":row[0],"TeamID":row[1],"PNo":row[2]})
  

with open('world-cup-goals-text.csv', encoding='utf-8') as f: 
    next(f)
    csv_f4 = csv.reader(f)
    for row in csv_f4:
        db.goals.insert_one({"GameID":row[0],"TeamID":row[1],"PNo":row[2],"Time":row[3],"Penalty":row[4]})
  

with open('stadiums-text.csv', encoding='utf-8') as f: 
    next(f)
    csv_f5 = csv.reader(f) 
    for row in csv_f5:
        db.stadium.insert_one({ "SID":row[0], "SName":row[1],"SCity":row[2],"SCapacity":row[3]})


with open('world-cup-teams-text.csv', encoding='utf-8') as f: 
    next(f)
    csv_f6 = csv.reader(f)
    for row in csv_f6:
        db.team_scores.insert_one({"TName" : row[1]})


#############################################
with open('world-cup-teams-text.csv', encoding='utf-8') as f: 
    next(f)
    csv_f1 = csv.reader(f)
    for row1 in csv_f1:
        for row in db.game.find({"$or":[{"TeamID1":row1[0]},{"TeamID2":row1[0]}]}):
            date= row['MatchDate']
            sid = str(row['SID'])
            team1id = str(row['TeamID1'])
            team2id = str(row['TeamID2'])
            team1score=str(row['Team1_score'])
            team2score=str(row['Team2_score'])
            cur2 = db.stadium.find({"SID":sid})
            for row in cur2:
                sname = str(row['SName'])
                scity = str(row['SCity'])
            cur3 =  db.team.find({"Team_ID":team1id})
            for row in cur3:
                t1name = str(row['Team_Name'])  
            cur4 =  db.team.find({"Team_ID":team2id})
            for row in cur4:
                t2name = str(row['Team_Name'])
            db.team_scores.update({"TName":row1[1]},{ "$push": { "Teamscores":{
                "GDate":date,
                "GCity":scity,
                "GStadium":sname,
                "T1Name":t1name,
                "T1Score":team1score,
                "T2Name":t2name,
                "T2Score":team2score
                }} })
        
with open('players-text.csv', encoding='latin1') as f: 
    next(f)
    csv_f7 = csv.reader(f)
    for row in csv_f7:
        db.player_data.insert_one({"PName" : row[4],"Team_Name" : row[0],"PNo" : row[2],"Position" : row[3]})


#############################################
with open('players-text.csv', encoding='latin1') as f: 
    next(f)
    csv_f = csv.reader(f)
    for row1 in csv_f:
        for row in db.game.find({"$or":[{"TeamID1":row1[1]},{"TeamID2":row1[1]}]}):
            count = 0
            gcount = 0
            gid = row['GameID']
            date= row['MatchDate']
            sid = str(row['SID'])
            team1id = str(row['TeamID1'])
            team2id = str(row['TeamID2'])
            cur2 = db.stadium.find({"SID":sid})
            for row in cur2:
                sname = str(row['SName'])
                scity = str(row['SCity'])
            cur3 =  db.team.find({"Team_ID":team1id})
            for row in cur3:
                t1name = str(row['Team_Name'])  
            cur4 =  db.team.find({"Team_ID":team2id})
            for row in cur4:
                t2name = str(row['Team_Name'])
            if t1name != row1[1]:
                opp = t1name
            elif t2name != row1[1]:
                opp = t2name
            gcount = db.goals.find({"$and":[{"TeamID":row1[1]},{"PNo":row1[2]}]}).count(True)
            flag=db.goals.find({"$and":[{"TeamID":row1[1]},{"PNo":row1[2]},{"GameID":gid}]}).count(True)
            for row in db.goals.find({"$and":[{"TeamID":row1[1]},{"PNo":row1[2]},{"GameID":gid}]}):
                gtime = row['Time']
                if row['Penalty'] == "Y":
                    gtype = "with Penalty"
                elif row['Penalty'] == "N":
                    gtype = "without Penalty"
                count= count+1
                if count == flag:
                    count=0
                break
            db.player_data.update({"PName" : row1[4],"Team_Name" : row1[0],"PNo" : row1[2],"Position" : row1[3]},{ "$push": { "PlayerData":{
                "GDate":date,
                "GCity":scity,
                "GStadium":sname,
                "T2Name":opp,
                "GoalType":gtype,
                "GoalTime": gtime
                }}})
        
teams_root = etree.Element("Team_Scores")

for game in db.game.find():
    Game_Element = etree.SubElement(teams_root,"Game")
    Team1_Name = etree.SubElement(Game_Element,"Team1_Name")
    Team1_Name.text = game['TeamID1']
    Team2_Name = etree.SubElement(Game_Element,"Team2_Name")
    Team2_Name.text = game['TeamID2']
    Team1_Score = etree.SubElement(Team1_Name,"Team1_Score")
    Team1_Score.text = game['Team1_score']
    Team2_Score = etree.SubElement(Team1_Name,"Team2_Score")
    Team2_Score.text = game['Team2_score']
    Stadium_Name = etree.SubElement(Game_Element,"Stadium_Name")
    Stadium_Name.text = game['SID']
    Player1 = etree.SubElement(Team1_Name,"Players1")
    Player2 = etree.SubElement(Team2_Name,"Players2")

xml = etree.tostring(teams_root, pretty_print=True)
with open("Teams.xml","wb") as file:
    file.write(xml)
    
players_root = etree.Element("Player_Data")

for game in db.player.find():
    Player_Element = etree.SubElement(teams_root,"Players")
    Player_Name = etree.SubElement(Player_Element,"Player_Name")
    Player_Name.text = game['PName']
    Player_No = etree.SubElement(Player_Element,"Player_No")
    Player_No.text = game['PNo']
    Team_Name = etree.SubElement(Player_Name,"Team_Name")
    Team_Name.text = game['Team_Name']
    Team_ID = etree.SubElement(Team_Name,"Team_ID")
    Team_ID.text = game['Team_ID']
    Position_Name = etree.SubElement(Player_Name,"League_Name")
    Position_Name = game['Position']

xml2 = etree.tostring(players_root, pretty_print=True)
with open("Players.xml","wb") as file:
    file.write(xml2)