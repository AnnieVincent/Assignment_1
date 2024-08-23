import psycopg2
import pymongo
import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Api key Connection:

def Api_connect():
    Api_key = "AIzaSyBf2UsdDbUltyOPKIltZdGuWiidunuvTyg"
    Api_service_name = "Youtube"
    Api_version = "V3"
    Youtube = build(Api_service_name,Api_version,developerKey=Api_key)
    return Youtube

Youtube=Api_connect()

# Getting Channel Info :

def Get_Channel_Info(Channel_ID):
    request = Youtube.channels().list(
                    part = "ContentDetails,statistics,snippet",
                    id = Channel_ID
    )
    response = request.execute()
    
    for i in range(0,len(response["items"])):
        data = dict(Channel_Name = response["items"][i]['snippet']['title'],
                    Channel_Id = response["items"][i]['id'],
                    Channel_Subscribers = response["items"][i]['statistics']['subscriberCount'],
                    Total_Number_of_videos = response["items"][i]['statistics']['videoCount'],
                    Total_Views = response["items"][i]['statistics']['viewCount'],
                    Channel_Description = response["items"][i]['snippet']['description'],
                    Playlist_Id = response["items"][i]['contentDetails']['relatedPlaylists']['uploads'])
    return data

# Getting Video Id's

def Get_Video_Ids(Channel_ID):
    video_Ids = []
    response = Youtube.channels().list(id = Channel_ID,
                                        part = 'contentDetails').execute()
    Playlist_Id =response_1["items"][0]['contentDetails']['relatedPlaylists']['uploads']

    next_Page_Token = None

    while True:
        response_1 = Youtube.playlistItems().list(
                                                part = 'snippet',
                                                playlistId = Playlist_Id,
                                                maxResults = 50,
                                                pageToken = next_Page_Token).execute()
        for i in range (len(response_1['items'])):
            video_Ids.append(response_1['items'][i]['snippet']['resourceId']['videoId'])
        next_Page_Token=response_1.get('nextPageToken')

        if next_Page_Token is None:
            break
    return video_Ids

# Getting Video Info : 

def Get_Video_Info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = Youtube.videos().list(
                part = "snippet,statistics,contentDetails",
                id = video_id
        )
        response = request.execute()
        for item in response['items']:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics'].get('viewCount'),
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favourites_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails'].get('definition'),
                        Caption_Status = item['contentDetails']['caption'])
            video_data.append(data)
    return video_data


# Getting Comment Info : 

def Get_Comment_Info(video_ids):
    
    Comment_Details = []

    for video_id in video_ids:
        try:
            request = Youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50
            )
            response = request.execute()

            for item in response["items"]:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published_on = item['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'])
                
                Comment_Details.append(data)
        except HttpError as e:
            if e.resp.status == 403:
                print(f"Comments are disabled for video ID {video_id}.")
            else:
                print(f"An error occured while fetching comments for video ID {video_id}: {e}")

    return Comment_Details


# Getting Playlist_Details : 

def Get_Playlist_Details(Channel_Id):
    next_page_token = None
    All_Data = []
    next_page = True
    while next_page:

        request = Youtube.playlists().list(
                    part = "snippet,contentDetails",
                    channelId = Channel_Id,
                    maxResults = 50,
                    pageToken = next_page_token 
        )
        response = request.execute()

        for item in response["items"]:
            data = dict(Playlist_Id = item['id'],
                        Title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        Channel_Name = item['snippet']['channelTitle'],
                        PublishedAt = item['snippet']['publishedAt'],
                        Video_count = item['contentDetails']['itemCount'])
            All_Data.append(data)
        
        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break
    return All_Data


# Connecting Mongo DB

client = pymongo.MongoClient("mongodb+srv://Annie:Annie1234@annie.dvfgudm.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_Project_DataBase"]

#  Uploading to MongoDB

def Channel_details(Channel_Id):
    Cha_Details = Get_Channel_Info(Channel_Id)
    Vid_Id = Get_Video_Ids(Channel_Id)
    Com_Details = Get_Comment_Info(Vid_Id)
    PL_Details = Get_Playlist_Details(Channel_Id)
    Vid_Info = Get_Video_Info(Vid_Id)

    Collection = db["Channel_Details"]
    Collection.insert_one({"Channel_Information":Cha_Details,"Comment_Information" : Com_Details,"Playlist_Information" : PL_Details,
                           "Video_Information" : Vid_Info})
    
    return "Upload Completed Successfully"


# Connecting To SQL and creating tables for Channels,Videos,Playlists,Comments 
def channels_table():
    My_db = psycopg2.connect(host = "localhost",
                            user = 'postgres',
                            password = '12/11/97',
                            database = 'Youtube_Project',
                            port = 5432)
    access = My_db.cursor()


    
    create_query = '''Create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_Id varchar(100) primary key,
                                                                Channel_Subscribers bigint,
                                                                Total_Number_of_videos bigint,
                                                                Total_Views bigint,
                                                                Channel_Description text,
                                                                Playlist_Id varchar(100))'''
    access.execute(create_query)
    My_db.commit()

    
# Getting channels values from mongo DB to postgres
        
    Channel_Details=[]
    db = client["Youtube_Project_DataBase"]
    collection = db["Channel_Details"]
    for Cha_Data in collection.find({},{"_id":0,"Channel_Information":1}):
        Channel_Details.append(Cha_Data["Channel_Information"])

    df = pd.DataFrame(Channel_Details)

    for index,row in df.iterrows():
            insert_query = '''insert into Channels(Channel_Name , 
                                                    Channel_Id,
                                                    Channel_Subscribers,
                                                    Total_Number_of_videos,
                                                    Total_Views,
                                                    Channel_Description,
                                                    Playlist_Id)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row["Channel_Name"],
                    row["Channel_Id"],
                    row["Channel_Subscribers"],
                    row["Total_Number_of_videos"],
                    row["Total_Views"],
                    row["Channel_Description"],
                    row["Playlist_Id"])
            
            access.execute(insert_query,values)
            My_db.commit()

            


# Uploading Playlist info into postgres

def playlist_table():
    My_db = psycopg2.connect(host = "localhost",
                            user = 'postgres',
                            password = '12/11/97',
                            database = 'Youtube_Project',
                            port = 5432)
    access = My_db.cursor()

    drop_query = '''drop table if exists Playlists'''
    access.execute(drop_query)
    My_db.commit()


    create_query = '''Create table if not exists Playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100) ,
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_count int
                                                        )'''

    access.execute(create_query)
    My_db.commit()

    Pl_list = []
    db = client["Youtube_Project_DataBase"]
    collection = db["Channel_Details"]
    for Pl_Data in collection.find({},{"_id":0,"Playlist_Information":1,}):
        for i in range(len(Pl_Data["Playlist_Information"])):
            Pl_list.append(Pl_Data["Playlist_Information"][i])
    df1 = pd.DataFrame(Pl_list)

    for index,row in df1.iterrows():
        insert_query = '''insert into Playlists(Playlist_Id, 
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_count
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        values = (row["Playlist_Id"],
                row["Title"],
                row["Channel_Id"],
                row["Channel_Name"],
                row["PublishedAt"],
                row["Video_count"])
                

        access.execute(insert_query,values)
        My_db.commit()


# Updating videos into table in postgres

def Videos_Table():
    My_db = psycopg2.connect(host = "localhost",
                            user = 'postgres',
                            password = '12/11/97',
                            database = 'Youtube_Project',
                            port = 5432)
    access = My_db.cursor()

    drop_query = '''drop table if exists Videos'''
    access.execute(drop_query)
    My_db.commit()


    create_query = '''Create table if not exists Videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_id varchar(100) primary key,
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(100),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favourites_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar(20)
                                                        )'''

    access.execute(create_query)
    My_db.commit()

    Vi_list = []
    db = client["Youtube_Project_DataBase"]
    collection = db["Channel_Details"]
    for Vi_Data in collection.find({},{"_id":0,"Video_Information":1,}):
        for i in range(len(Vi_Data["Video_Information"])):
            Vi_list.append(Vi_Data["Video_Information"][i])
    df2 = pd.DataFrame(Vi_list)

    for index,row in df2.iterrows():
            insert_query = '''insert into Videos(Channel_Name,
                                                Channel_Id,
                                                    Video_id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favourites_Count,
                                                    Definition,
                                                    Caption_Status)

                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values=(row["Channel_Name"],
                    row["Channel_Id"],
                    row["Video_id"],
                    row["Title"],
                    row["Tags"],
                    row["Thumbnail"],
                    row["Description"],
                    row["Published_Date"],
                    row["Duration"],
                    row["Views"],
                    row["Likes"],
                    row["Comments"],
                    row["Favourites_Count"],
                    row["Definition"],
                    row["Caption_Status"]
                    )
                    

            access.execute(insert_query,values)
            My_db.commit()


# Updating Comments into table in postgres

def Comments_Table():
        My_db = psycopg2.connect(host = "localhost",
                                user = 'postgres',
                                password = '12/11/97',
                                database = 'Youtube_Project',
                                port = 5432)
        access = My_db.cursor()

        drop_query = '''drop table if exists Comments'''
        access.execute(drop_query)
        My_db.commit()

        try:
            create_query = '''Create table if not exists Comments(Comment_Id varchar (100) primary key,
                                                                    Video_id varchar (100),
                                                                    Comment_Author varchar (200),
                                                                    Comment_Published_on timestamp,
                                                                    Comment_Text text
                                                            )'''

            access.execute(create_query)
            My_db.commit()

        except:
            st.write("CommentsTable already created")

        Comment_list = []
        db = client["Youtube_Project_DataBase"]
        collection = db["Channel_Details"]
        for Comment_Data in collection.find({},{"_id":0,"Comment_Information":1,}):
                for i in range(len(Comment_Data["Comment_Information"])):
                        Comment_list.append(Comment_Data["Comment_Information"][i])
        df3 = pd.DataFrame(Comment_list)


        for index,row in df3.iterrows():
                insert_query = '''insert into Comments(Comment_Id,
                                                        Video_id,
                                                        Comment_Author,
                                                        Comment_Published_on,
                                                        Comment_Text
                                                        )

                                                        values(%s,%s,%s,%s,%s)'''
                values=(row["Comment_Id"],
                        row["Video_id"],
                        row["Comment_Author"],
                        row["Comment_Published_on"],
                        row["Comment_Text"],
                        )
                        
                access.execute(insert_query,values)
                My_db.commit()

def tables_upload():

    channels_table()
    playlist_table()
    Videos_Table()
    Comments_Table()

    return "Tables created Successfully"    


def show_Channels_Table():
    cha_list = []
    db = client["Youtube_Project_DataBase"]
    collection = db["Channel_Details"]
    for Cha_Data in collection.find({},{"_id":0,"Channel_Information":1,}):
        cha_list.append(Cha_Data["Channel_Information"])
    df = st.dataframe(cha_list)

    return df

def show_Playlists_Table():
    Pl_list = []
    db = client["Youtube_Project_DataBase"]
    collection = db["Channel_Details"]
    for Pl_Data in collection.find({},{"_id":0,"Playlist_Information":1,}):
        for i in range(len(Pl_Data["Playlist_Information"])):
            Pl_list.append(Pl_Data["Playlist_Information"][i])
    df1 = st.dataframe(Pl_list)

    return df1

def show_Video_Table():
    Vi_list = []
    db = client["Youtube_Project_DataBase"]
    collection = db["Channel_Details"]
    for Vi_Data in collection.find({},{"_id":0,"Video_Information":1,}):
        for i in range(len(Vi_Data["Video_Information"])):
            Vi_list.append(Vi_Data["Video_Information"][i])
    df2 = st.dataframe(Vi_list)

    return df2

def show_Comments_Table():
    Comment_list = []
    db = client["Youtube_Project_DataBase"]
    collection = db["Channel_Details"]
    for Comment_Data in collection.find({},{"_id":0,"Comment_Information":1,}):
            for i in range(len(Comment_Data["Comment_Information"])):
                    Comment_list.append(Comment_Data["Comment_Information"][i])
    df3 = st.dataframe(Comment_list)

    return df3


 #### STREAMLIT SETTINGS

st.balloons()
st.header("YouTube Data Harvesting and Warehousing",divider='rainbow')

with st.sidebar:
    st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header(":blue[PROJECT_1]")
    st.caption(":red[Python Scripting]")
    st.caption(":red[Data Collection]")
    st.caption(":red[API_Integration]")
    st.caption(":red[SQL]")
    st.caption(":red[MongoDB]")
    st.caption(":red[Data Management with MongoDB and SQL]")

Channel_ID = st.text_input("Enter the Channel ID")

if st.button(":red[Collect and store data]"):
    Cha_Ids = []
    db = client["Youtube_Project_DataBase"]
    Collection = db["Channel_Details"]
    for Cha_data in Collection.find({},{"_id":0,"Channel_Information":1}):
        Cha_Ids.append(Cha_data["Channel_Information"]["Channel_Id"])

    if Channel_ID in Cha_Ids:
        st.success(":green[Channels details of the entered channel already exists]")
    else:
        insert = Channel_details(Channel_ID)
        st.success(insert)

from psycopg2 import IntegrityError

client = pymongo.MongoClient("mongodb+srv://Annie:Annie1234@annie.dvfgudm.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_Project_DataBase"]
collection = db["Channel_Details"]

My_db = psycopg2.connect(host = "localhost",
                            user = 'postgres',
                            password = '12/11/97',
                            database = 'Youtube_Project',
                            port = 5432)
access = My_db.cursor()

def migrate_to_sql(channel_name):

    channel_data = collection.find_one({"Channel_Information.Channel_Name": channel_name}, {"_id": 0})
    try:
        if channel_data:
            columns = ", ".join(channel_data["Channel_Information"].keys())
            values_template = ", ".join(["%s"] * len(channel_data["Channel_Information"]))
            values = tuple(channel_data["Channel_Information"].values())
            

            access.execute(
                f"""
                INSERT INTO channels ({columns})
                VALUES ({values_template})
                """,
                values
            )
            My_db.commit()
            return "All Data Migrated to Postgres SQL Tables Created Successfully."
        
    except IntegrityError as e:
        return ":red[This Channel details already exists.Please Select the Other Valid Channels Details.]"


All_channels = []
db = client["Youtube_Project_DataBase"]
collection = db["Channel_Details"]
for Cha_Data in collection.find({},{"_id":0,"Channel_Information":1,}):
    All_channels.append(Cha_Data['Channel_Information']['Channel_Name'])

Unique_Channels = st.selectbox("Select the Channel",All_channels)

if st.button(":violet[Migrate to SQL]"):
    result = migrate_to_sql(Unique_Channels)
    st.success(result)

show_Table= st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_Table == ":red[CHANNELS]":
    show_Channels_Table()

elif show_Table == ":green[PLAYLISTS]":
    show_Playlists_Table()

elif show_Table == ":orange[COMMENTS]":
    show_Comments_Table()

elif show_Table == ":blue[VIDEOS]":
    show_Video_Table() 


# SQL Connection

My_db = psycopg2.connect(host = "localhost",
                        user = 'postgres',
                        password = '12/11/97',
                        database = 'Youtube_Project',
                        port = 5432)
access = My_db.cursor()

Question = st.selectbox("SELECT YOUR QUESTION",("1. All videos and their channels.",
                                                "2. Channels with the most videos are listed with their counts.",
                                                "3. Top 10 viewed videos and their channels.",
                                                "4. Comments on each video are detailed with their titles.",
                                                "5. Videos with the highest likes.",
                                                "6. Total likes for each video along with their titles.",
                                                "7. Total views for each channel .",
                                                "8. Channels that published videos in 2022.",
                                                "9. Average videos duration of each channel.",
                                                "10. Videos with the highest number of comments."))

if Question == "1. All videos and their channels.":
    Query_1 = '''select title as videos,channel_name as channelname from videos'''
    access.execute(Query_1)
    My_db.commit()
    Table_1 = access.fetchall()
    df = pd.DataFrame(Table_1,columns = ["Video Title","Channel Name"])
    st.write(df)

elif Question == "2. Channels with the most videos are listed with their counts.":
    Query_2 = '''select channel_name as channelname,Total_Number_of_videos as No_of_videos from channels
                    order by Total_Number_of_videos'''
    access.execute(Query_2)
    My_db.commit()
    Table_2 = access.fetchall()
    df_2 = pd.DataFrame(Table_2,columns = ["Channel Name","Total No Of Videos"])
    st.write(df_2)

elif Question == "3. Top 10 viewed videos and their channels.":
    Query_3 = '''select views as Views,channel_name as channelname,title as videotitle from videos
                    where views is not null order by views desc limit 10'''
    access.execute(Query_3)
    My_db.commit()
    Table_3 = access.fetchall()
    df_3 = pd.DataFrame(Table_3,columns = ["Views","Channel Name","Video Title"])
    st.write(df_3)

elif Question == "4. Comments on each video are detailed with their titles.":
    Query_4 = '''select Comments as no_of_comments,title as videotitle from videos where comments is not null
                    '''
    access.execute(Query_4)
    My_db.commit()
    Table_4 = access.fetchall()
    df_4 = pd.DataFrame(Table_4,columns = ["Comments","Video Title"])
    st.write(df_4)

elif Question == "5. Videos with the highest likes.":
    Query_5 = '''select title as videotitle,channel_name as channelname , likes as highestlikes from videos 
                    where Likes is not null order by Likes desc
                    '''
    access.execute(Query_5)
    My_db.commit()
    Table_5 = access.fetchall()
    df_5 = pd.DataFrame(Table_5,columns = ["Video Title","Channel Name","Likes"])
    st.write(df_5)


elif Question == "6. Total likes for each video along with their titles.":
    Query_6 = '''select title as videotitle, likes as totallikes from videos 
                    where Likes is not null 
                    '''
    access.execute(Query_6)
    My_db.commit()
    Table_6 = access.fetchall()
    df_6 = pd.DataFrame(Table_6,columns = ["Video Title","Likes"])
    st.write(df_6)

elif Question == "7. Total views for each channel .":
    Query_7 = '''select channel_name as channelname, Total_Views as totalviews from channels  
                    '''
    access.execute(Query_7)
    My_db.commit()
    Table_7 = access.fetchall()
    df_7 = pd.DataFrame(Table_7,columns = ["Channel Name","Views"])
    st.write(df_7)

elif Question == "8. Channels that published videos in 2022.":
    Query_8 = '''select title as Videostitle , channel_name as channelname, Published_Date as Videopublished from videos  
                    where extract(year from Published_Date) = 2022'''
    access.execute(Query_8)
    My_db.commit()
    Table_8 = access.fetchall()
    df_8 = pd.DataFrame(Table_8,columns = ["Video Title","Channel Name","Published Date"])
    st.write(df_8)

elif Question == "9. Average videos duration of each channel.":
    Query_9 = '''select channel_name as channelname,AVG (Duration) as Averageduration from videos  
                    group by channel_name'''
    access.execute(Query_9)
    My_db.commit()
    Table_9 = access.fetchall()
    df_9 = pd.DataFrame(Table_9,columns = ["Channel Name","Average Duration"])
    df_9

    T9=[]
    for index,row in df_9.iterrows():
        Channel_Name = row["Channel Name"]
        Average_Duration = row["Average Duration"]
        Average_Duration_string = str(Average_Duration)
        T9.append(dict(Channelname = Channel_Name,Avgduration = Average_Duration_string))
    df1 = pd.DataFrame(T9)
    st.write(df1)

elif Question == "10. Videos with the highest number of comments.":
    Query_10 = '''select title as videotitle,channel_name as channelname,comments as comments from videos  
                    where comments is not null order by comments desc'''
    access.execute(Query_10)
    My_db.commit()
    Table_10 = access.fetchall()
    df_10 = pd.DataFrame(Table_10,columns = ["Video Title","Channel Name","Total Comments"])
    st.write(df_10)

            