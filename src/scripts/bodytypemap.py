import csv
import json
import pandas as pd
import numpy as np
from cmath import e
import pymongo
from bson.son import SON
from pymongo import MongoClient, InsertOne
from getpass import getpass
from mysql.connector import connect, Error
import numpy as np
import pandas as pd
import configparser
from six.moves import configparser
import io
import csv
import psycopg2

 

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
        bodytypeid = 1
        # creating a blank series
        bodytype_new = [] #pd.Series([()])
        bodytypeid_new = []
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
                bodytypeid = 1
            elif(isTopHourglass(bustlen,waistLen,hiplen)):
                bodytype = "tophourglass"
                bodytypeid = 3
            elif(isBottomHourglass(bustlen,waistLen,highhiplen,hiplen)):
                bodytype = "bottomhourglass"
                bodytypeid = 2
            elif(isTriangle(bustlen,waistLen,highhiplen,hiplen)):
                bodytype = "triangle"
                bodytypeid = 4
            elif(isInvertedTriangle(bustlen,waistLen,hiplen)):
                bodytype = "invertedtriangle"
                bodytypeid = 6
            elif(isRectange(bustlen,waistLen,hiplen)):
                bodytype = "rectangle"
                bodytypeid = 7
            elif(isRound(bustlen,waistLen,hiplen)):
                bodytype = "round"
                bodytypeid = 5

            print("bodytype",bodytype)
            if (bodytype != ''):
                bodytype_new.append(bodytype)
                bodytypeid_new.append(bodytypeid)
                print("sshhhh",bodytype_new)
            else:
                bodytype_new.append("hourglass")
                bodytypeid_new.append(bodytypeid)
            
            i +=1   
            # inserting new column with values of list made above       
            
        #print(data)
        df.insert(7, "body_type", bodytype_new)
        df.insert(1,"body_type_id",bodytypeid_new)
        print(df)
        df.head()
        df.to_csv('C:/Users/bhati/Documents/DB 225/DBSemProject/bodyMeasureAndType.csv')
        #print(df)    
    
def cleanArticleTable():
    df = pd.read_csv(r'C:/Users/bhati/Documents/DB 225/DBSemProject/modifiedArticleType.csv')#,skiprows=1,header=None,usecols=[0,1,2,3,4,5,8,9,24])
    print(df.shape)
    # Get names of indexes for which column product_group_name has value Accessories
    indexNames = df[ df['5'] == 'Accessories' ].index
    print(indexNames)
    # Delete these row indexes from dataFrame
    df.drop(df.loc[df['5']=='Accessories'].index, inplace=True)
    df.drop(df.loc[df['5'] == 'Underwear'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Socks & Tights'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Items'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Nightwear'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Unknown'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Underwear/nightwear'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Shoes'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Swimwear'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Cosmetic'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Interior textile'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Bags'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Furniture'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Garment and Shoe care'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Fun'].index,inplace=True)
    df.drop(df.loc[df['5'] == 'Stationery'].index,inplace=True)
                  
    #df.drop(indexNames , inplace=True)
    
    df.to_csv('C:/Users/bhati/Documents/DB 225/DBSemProject/delExtraGrpsArticleType.csv')

def checkColorValues():
    df = pd.read_csv(r'C:/Users/bhati/Documents/DB 225/DBSemProject/delExtraGrpsArticleType.csv')#,skiprows=1,header=None,usecols=[0,1,2,3,4,5,8,9,24])
    print("abc",df.shape)
    print(df.head)
    print(df['9'].unique())
    new_df = df[['8','9']].copy()
    new_df.to_csv('C:/Users/bhati/Documents/DB 225/DBSemProject/colorType.csv')


def getConnection():
    config = configparser.RawConfigParser(allow_no_value=True)
        
    config.read('C:/Users/bhati/Documents/DB 225/DB HW/DB HW3/HW3/code_python/databasecfg.cnfg')
    configsec="mysql"

    hostcnfg =config.get(configsec, 'db.host') #localhost
    portcnfg =config.get(configsec,'db.port') #3306
    usercnfg = config.get(configsec, 'db.user') #input("Enter username: "),
    passwordcnfg = config.get(configsec, 'db.password') #getpass("Enter password: ")
    databasecnfg = config.get(configsec, 'db.current.database')
    print(configsec,hostcnfg,usercnfg,passwordcnfg,databasecnfg)
    connection = connect(user=usercnfg, password=passwordcnfg,
                              host=hostcnfg,
                              port=portcnfg,
                              database=databasecnfg)
    print("Mysql connection is..", connection); 
    create_db_query = "USE projectdata225"
    with connection.cursor() as cursor:
        cursor.execute(create_db_query)

    return connection


inserted_product_code=[]
inserted_article=[]
inserted_product_style_map=[()]

def insertProduct(connection , productInfo):
    # for index, row in productInfo.iterrows():
    #     if row['1'] not in inserted_product_code :
    #         datarow=[row['1'] , row['3'] , row['2'] , row['24']]
    #         print("Inserting product " , str(datarow))
    #         with connection.cursor() as cursor:
    #             cursor.execute('INSERT INTO product(product_code, product_type_id, prod_name, detail_desc)' 'VALUES(%s, %s, %s, %s)', datarow)
    #             connection.commit()
    #         inserted_product_code.append(row['1'])
    #     insertProductColor(connection , row['0'] , row['1'] , row['8'])
    return 0


def insertProductColor(connection , articleid , productId , colorId):
    # if articleid not in inserted_article :
    #     datarow=[articleid , productId , colorId ]
    #     print("Inserting product article with new color" , str(datarow))
    #     with connection.cursor() as cursor:
    #         cursor.execute('INSERT INTO product_color_map(article_id , product_code, product_color_id)' 'VALUES(%s, %s, %s)', datarow)
    #         connection.commit()
    #     inserted_article.append(articleid)
    return 0


def insertProductStyleMap(connection, productInfo , stypeInfo):
    #print("Inside Product Style    ", stypeInfo[0],stypeInfo[1], productInfo['1'],productInfo.shape)
    for index, row in productInfo.iterrows():
        if (stypeInfo[1],row['1']) not in inserted_product_style_map:
            datarow=[stypeInfo[1],row['1']]
            print("Inserting product style map" , str(datarow))
            print("aaaaa",stypeInfo[1],row['1'])
            with connection.cursor() as cursor:
                cursor.execute('INSERT INTO product_style_map(product_style_id, product_code)' 'VALUES(%s, %s)', datarow)
                connection.commit()
            inserted_product_style_map.append((stypeInfo[1],row['1']))
    return 0





def findStyles(connection):

    queryfindStyle="""select style_name , product_style_id from product_style"""

    with connection.cursor() as cursor:
        cursor.execute(queryfindStyle)
        result = cursor.fetchall()
        df = pd.read_csv(r'C:/Users/bhati/Documents/DB 225/DBSemProject/delExtraGrpsArticleType.csv')
        df_1 = []
        df = df.replace({np.nan: None})
        for c in df.columns:
            count = df[c].isnull().sum()
            print(f'Col {c} has {count} missing values')
        print(f'Done checking for missings')
        #insertProduct(connection , df)  
        print("Finding styles in articles:")
        for row in result:
            style = row[0]
            df_new = df[df['24'].str.contains(style, regex=True, na=False)]
            print("Searching for Style : " + style)
            if df_new.empty == False :
                #print(df_new)
                insertProductStyleMap(connection , df_new , row)  
                df_1.append(df_new)
                
            #print(row)

        
        #print("xxxxxx: ", df_1)


def loadCustomerTransactionDump():
    df = pd.read_csv(r'C:/Users/bhati/Documents/DB 225/DBSemProject/transactions_train.csv',skiprows=1,header=None,usecols=[0,1,2,3])
    df.to_csv('C:/Users/bhati/Documents/DB 225/DBSemProject/transaction_train_wo_saleschannel.csv')


def insertcustomer(connection):
    
    df = pd.read_csv(r'C:/Users/bhati/Documents/DB 225/DBSemProject/bodyMeasureAndType.csv',skiprows=1,header=None)
    print(df)
    df = df.replace({np.nan: None})
    for row in df.itertuples():
        print("row..",row[2],row[3])
        datarow=[row[2] , row[3] , row[4] , row[5],row[6] , row[7] , row[8] , row[9],row[10]]
        print("datarow:",datarow)
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO customer(customer_id,body_type_id,gender,age,bust,highhip,waist,hip,bodytype)' 'VALUES(%s, %s, %s,%s,%s,%s,%s,%s,%s)', datarow)
            connection.commit()





try:
    #convertCSVtoJSON()
    #loadJSONtoMongoDB()
    #loadBodyMapFromMongo()
    #importCustProfileCSV()
    #cleanArticleTable()
    #checkColorValues()
    connection = getConnection()
    #findStyles(connection)
    #loadCustomerTransactionDump()
    insertcustomer(connection)

except Exception as exception:
    print(exception)
