import csv
import json
import pandas as pd
import numpy as np
from cmath import e
import pymongo
from bson.son import SON
from pymongo import MongoClient, InsertOne
 

urlDocker="mongodb://localhost:49153/ProjectData225" # docker Instance
 
# Function to convert a CSV to JSON
# Takes the file paths as arguments
def make_json(csvFilePath, jsonFilePath):
     
    # create a dictionary
    data = {}
     
    # Open a csv reader called DictReader
    with open(csvFilePath, encoding='utf-8-sig') as csvf:
        csvReader = csv.DictReader(csvf)
         
        # Convert each row into a dictionary
        # and add it to data
        for rows in csvReader:
            print("roooows", rows)
            # Assuming a column named 'ratiosid' to
            # be the primary key
            key = rows['ratiosid']
            data[key] = rows
 
    # Open a json writer, and use the json.dumps()
    # function to dump data
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))
         


def convertCSVtoJSON():
    # Driver Code
 
    # Decide the two file paths according to your
    # computer system
    csvFilePath = r'C:/Users/bhati/Documents/DB 225/DBSemProject/bodytypemapping.csv'
    jsonFilePath = r'C:/Users/bhati/Documents/DB 225/DBSemProject/bodytypemapping.json'
 
    # Call the make_json function
    make_json(csvFilePath, jsonFilePath)

def loadJSONtoMongoDB():
    myclient = pymongo.MongoClient(urlDocker)  
    print(myclient.list_database_names())
    mydb = myclient['ProjectData225']
    collection = mydb['bodytype']
    print(collection)

    with open(r"C:/Users/bhati/Documents/DB 225/DBSemProject/bodytypemapping.json") as file:
        file_data = json.load(file)
      
        # Inserting the loaded data in the Collection
        # if JSON contains data more than one entry
        # insert_many is used else inser_one is used
        if isinstance(file_data, list):
            collection.insert_many(file_data)  
        else:
            collection.insert_one(file_data)
            
    myclient.close()


def isHourglass(bustlen,waistLen,hiplen):
    print("Inside isHourglass",bustlen,waistLen,hiplen)
    #(bust – hips) ≤ 1″ AND (hips – bust) < 3.6″ AND (bust – waist) ≥ 9″ OR (hips – waist) ≥ 10″
    if ((float(bustlen)-float(hiplen)) <= 1.0) and ((float(hiplen)-float(bustlen)) < 3.6 ) and (((float(bustlen)-float(waistLen)) >= 9.0) or ((float(hiplen)-float(waistLen)) >= 10.0)):
        print("hourglass")
        return True

def isTopHourglass(bustlen,waistLen,hiplen):
    print("Inside isTopHourglass",bustlen,waistLen,hiplen)
    #top hourglass is: (bust – hips) > 1″ AND (bust – hips) < 10″ AND (bust – waist) ≥ 9″
    if ((float(bustlen)-float(hiplen)) > 1.0) and ((float(bustlen)-float(hiplen)) < 10.0) and ((float(bustlen)-float(waistLen)) >= 9.0):
        print("tophourglass")
        return True

def isBottomHourglass(bustlen,waistLen,highhiplen,hiplen):
    print("Inside isBottomHourglass",bustlen,waistLen,highhiplen,hiplen)
    #bottom hourglass is referred to as: (hips – bust) ≥ 3.6″ AND (hips – bust) < 10″ AND (hips – waist) ≥ 9″ AND (high hip/waist) < 1.193
    if ((float(hiplen)-float(bustlen)) >= 3.6) and ((float(hiplen)-float(bustlen)) < 10.0) and ((float(hiplen)-float(waistLen)) >= 9.0) and ((float(highhiplen)/float(waistLen)) < 1.193):
        print("bottomhourglass")
        return True


def isTriangle(bustlen,waistLen,highhiplen,hiplen):
    print("Inside isTriangle",bustlen,waistLen,highhiplen,hiplen)
    #Triangle/Spoon is referred to as: If (hips – bust) > 2″ AND (hips – waist) ≥ 7″ AND (high hip/waist) ≥ 1.193. 16/17/12/28
    if ((float(hiplen)-float(bustlen)) > 2.0) and ((float(hiplen)-float(waistLen)) >= 7.0) and ((float(highhiplen)/float(waistLen)) >= 1.193):
        print("triangle")
        return True


def isInvertedTriangle(bustlen,waistLen,hiplen):
    print("Inside invertedTriangle",bustlen,waistLen,hiplen)
    #If (bust – hips) ≥ 3.6″ AND (bust – waist) < 9″.
    if ((float(bustlen)-float(hiplen)) >= 3.6 ) and ((float(bustlen)-float(waistLen)) < 9.0):
        print("invertedtriangle")
        return True

def isRectange(bustlen,waistLen,hiplen):
    print("Inside isRectange",bustlen,waistLen,hiplen)
    #(hips – bust) < 3.6″ AND (bust – hips) < 3.6″ AND (bust – waist) < 9″ AND (hips – waist) < 10″. 
    if ((float(hiplen)-float(bustlen)) < 3.6) and ((float(bustlen)-float(hiplen)) < 3.6) and ((float(bustlen)-float(waistLen)) < 9.0) and ((float(hiplen)-float(waistLen)) < 10.0 ):
        print("rectangle")
        return True

def isRound(bustlen,waistLen,hiplen):
    print("Inside isRound",bustlen,waistLen,hiplen)
    #(hips – bust) ≥ 3.6″ AND (hips – waist) < 9.
    if ((float(hiplen)-float(bustlen)) >= 3.6) and ((float(hiplen)-float(waistLen)) < 9.0):
        print("round")
        return True




def importCustProfileCSV():
    
    client = pymongo.MongoClient("mongodb://localhost:49153/ProjectData225")
 
    # Database Name
    db = client["ProjectData225"]
 
    # Collection Name
    col = db["bodytype"]
 
    # Fields with values as 1 will
    # only appear in the result
    x = col.find({},{'_id':0,'ratiosid': 1, 'bodytype': 1,'bust-hips_min':1,'bust-hips_max':1,'hips-bust_min':1,'hips-bust_max':1,
                    'bust-waist_min':1,'bust-waist_max':1,'hips-waist':1,'high hip/waist':1})
    
    for data in x:
        df = pd.read_csv(r'C:/Users/bhati/Documents/DB 225/DBSemProject/bodymeasurements.csv',skiprows=1,header=None,usecols=[0,1,2,5,6,7,8])
        print(df)
        i=0
        bodytype = ""

        # creating a blank series
        bodytype_new = pd.Series([])

        #df.insert(5, "bodytype", "hourglass")
        print("after adding empty column",df.shape[0])

        for x in range(df.shape[0]):
            print("abc", x)
        
            waistLen = df[7][i]
            bustlen = df[5][i]
            highhiplen = df[6][i]
            hiplen = df[8][i]
            
            print("body",bustlen,waistLen,hiplen,highhiplen)

            if(isHourglass(bustlen,waistLen,hiplen)):
                bodytype = "hourglass"
            elif(isTopHourglass(bustlen,waistLen,hiplen)):
                bodytype = "tophourglass"
            elif(isBottomHourglass(bustlen,waistLen,highhiplen,hiplen)):
                bodytype = "bottomhourglass"
            elif(isTriangle(bustlen,waistLen,highhiplen,hiplen)):
                bodytype = "triangle"
            elif(isInvertedTriangle(bustlen,waistLen,hiplen)):
                bodytype = "invertedtriangle"
            elif(isRectange(bustlen,waistLen,hiplen)):
                bodytype = "rectangle"
            elif(isRound(bustlen,waistLen,hiplen)):
                bodytype = "round"

            print("bodytype",bodytype)
            if (bodytype != ''):
                bodytype_new[i] = bodytype
            else:
                bodytype_new[i] = "hourglass"
            
            i +=1
            # inserting new column with values of list made above       
            
        #print(data)
        df.insert(7, "BodyType", bodytype_new)
        print(df)
        df.head()
        df.to_csv('C:/Users/bhati/Documents/DB 225/DBSemProject/bodyMeasureAndType.csv')
        #print(df)    
    



try:
    #convertCSVtoJSON()
    #loadJSONtoMongoDB()
    #loadBodyMapFromMongo()
    importCustProfileCSV()

except Exception as exception:
    print(exception)
