from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API Key connection 

def Api_connect():
    Api_Id="YOUR_API_KEY"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()

def get_channel_info(channel_id):

    request=youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Vedios=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data
      

def get_videos_ids(channel_id):

    video_ids=[]

    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:

       
        response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_Id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
          video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
          break
    return video_ids

def get_video_info(video_ids):

        video_data=[]

        for video_id in video_ids:
            request=youtube.videos().list(
                part="snippet,ContentDetails,statistics",
                id=video_id
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet'].get('description'),
                        Published_Date=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics'].get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
                        )
                video_data.append(data)
        return video_data


def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
                request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50
                )
                response=request.execute()

                for item in response['items']:
                    data=dict(Comment_Id=item['snippet']['topLevelComment']['id'] ,
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                            )
                    Comment_data.append(data)
    except:
        pass
    return Comment_data


def get_playlist_details(channel_id):

    next_page_token=None
    All_data=[]
    while True:

        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)
        
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break

    return All_data


client=pymongo.MongoClient("YOUR_MONGODB_CONNECTION")
db=client["Youtube_data"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one(
        {
            "channel_information":ch_details,
            "playlist_information":pl_details,
            "video_information":vi_details,
            "comment_information":com_details
        }
    )

    return "upload completed successfully"
    

def channel_table():

        mydb=psycopg2.connect(host="localhost",
                            user="YOUR_USERS",
                            password="YOUR_PASSWORD",
                            database="youtube_data",
                            port="5432")
        cursor=mydb.cursor()


        drop_query='''drop table if exists channels'''
        cursor.execute(drop_query)
        mydb.commit()

        try:
            create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_Id varchar(80) primary key ,
                                                                Subscribers bigint,
                                                                Views bigint,
                                                                Total_Videos int,
                                                                Channel_Description text,
                                                                Playlist_Id varchar(80))'''
            
            cursor.execute(create_query)

            
            mydb.commit()



        except:
            print("channels table already created")

        ch_list=[]


        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_list.append(ch_data["channel_information"])

        df=pd.DataFrame(ch_list)
        df.rename(columns={'Total_Vedios': 'Total_Videos'}, inplace=True)



        for index,row in df.iterrows():
            insert_query='''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers ,
                                            Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)
                                                    ON CONFLICT (Channel_Id) DO NOTHING'''
            
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Views'],
                    row['Total_Videos'],
                    row['Channel_Description'],
                    row['Playlist_Id'])
            
            try:
                cursor.execute(insert_query,values)
                mydb.commit()

            except:
                print("channels values are already inserted ")

def playlist_table():

    mydb=psycopg2.connect(host="localhost",
                            user="YOUR_USERS",
                            password="YOUR_PASSWORD",
                            database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()


    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100) ,
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        video_Count int )'''

    cursor.execute(create_query)
    mydb.commit()
    
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
         pl_list.append(pl_data["playlist_information"][i])


    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id ,
                                            Channel_Name,
                                            PublishedAt,
                                            video_Count
                                              )
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
         #ON CONFLICT (Playlist_Id) DO NOTHING
        
        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['video_Count']
                )
        
        cursor.execute(insert_query,values)
        mydb.commit()




def videos_table():

    mydb=psycopg2.connect(host="localhost",
                            user="YOUR_USERS",
                            password="YOUR_PASSWORD",
                            database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()


    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(100),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50)) '''



    cursor.execute(create_query)
    mydb.commit()


    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=pd.DataFrame(vi_list)
    
    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Published_Date,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Comments,
                                                        Favorite_Count,
                                                        Definition,
                                                        Caption_Status
                                                    )
                                                
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            #ON CONFLICT (Playlist_Id) DO NOTHING
        

            
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status']
                )
            
        cursor.execute(insert_query,values)
        mydb.commit()



def comments_table():


    mydb=psycopg2.connect(host="localhost",
                            user="YOUR_USERS",
                            password="YOUR_PASSWORD",
                            database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()


    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''
                    
    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published
                                                )
                                            
                                            values(%s,%s,%s,%s,%s)'''
            #ON CONFLICT (Comment_Id) DO NOTHING
        
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
                )
            
        cursor.execute(insert_query,values)
        mydb.commit()

def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()

    return "tables created successfully"

def show_channels_table():

    ch_list=[]

    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=st.dataframe(ch_list)
    # df.rename(columns={'Total_Vedios': 'Total_Videos'}, inplace=True)
    return df

def show_playlists_table():

    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])


    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():

    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():

    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3=st.dataframe(com_list)

    return df3

# #streamlit part

#user friendly ui
#@@@@@@@



st.set_page_config(page_title="YouTube Data Harvesting", page_icon=":tv:", layout="wide")


with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.subheader("Developed by Saravanan")
    st.write("Welcome to the YouTube Data Harvesting tool. Use this tool to collect, store, and analyze YouTube channel data.")
    
 
    st.header(":mag: **Data Collection**")
    channel_id = st.text_input("Enter The Channel ID", help="Enter the YouTube Channel ID you want to collect data for.")
    
    if st.button("Collect and Store Data"):
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])

        if channel_id in ch_ids:
            st.warning("Channel details for the given Channel ID already exist.")
        else:
            insert = channel_details(channel_id) 
            st.success(insert)
    
    st.header(":inbox_tray: **Data Migration**")
    if st.button("Migrate to SQL"):
        Table = tables()  
        st.success(Table)

st.header(":bar_chart: **Data Visualization & Queries**")
show_table = st.radio("Select the table to view:", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    show_channels_table()  
elif show_table == "PLAYLISTS":
    show_playlists_table()  
elif show_table == "VIDEOS":
    show_videos_table() 
elif show_table == "COMMENTS":
    show_comments_table() 

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Iamsaro@.1234#",
    database="youtube_data",
    port="5432"
)
cursor = mydb.cursor()

st.header(":scroll: **Query the Data**")
question = st.selectbox("Select Your Query", [
    "1. All the videos and the channel names",
    "2. Channels with the most number of videos",
    "3. 10 most viewed videos",
    "4. Comments on each video",
    "5. Videos with the highest likes",
    "6. Likes of all videos",
    "7. Views of each channel",
    "8. Videos published in the year 2022",
    "9. Average duration of all videos in each channel",
    "10. Videos with the highest number of comments"
])

if question == "1. All the videos and the channel names":
    query1 = '''SELECT title AS videos, channel_name AS channelname FROM videos'''
    cursor.execute(query1)
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns=["Video Title", "Channel Name"])
    st.write(df)

elif question == "2. Channels with the most number of videos":
    query2 = '''SELECT channel_name AS channelname, total_videos AS no_videos FROM channels ORDER BY total_videos DESC'''
    cursor.execute(query2)
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["Channel Name", "Number of Videos"])
    st.write(df2)

elif question == "3. 10 most viewed videos":
    query3 = '''SELECT views AS views, channel_name AS channelname, title AS videotitle FROM videos WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["Views", "Channel Name", "Video Title"])
    st.write(df3)

elif question == "4. Comments on each video":
    query4 = '''SELECT comments AS no_comments, title AS videotitle FROM videos WHERE comments IS NOT NULL'''
    cursor.execute(query4)
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["Number of Comments", "Video Title"])
    st.write(df4)

elif question == "5. Videos with the highest likes":
    query5 = '''SELECT title AS videotitle, channel_name AS channelname, likes AS likecount FROM videos WHERE likes IS NOT NULL ORDER BY likes DESC'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["Video Title", "Channel Name", "Like Count"])
    st.write(df5)

elif question == "6. Likes of all videos":
    query6 = '''SELECT likes AS likecount, title AS videotitle FROM videos'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["Like Count", "Video Title"])
    st.write(df6)

elif question == "7. Views of each channel":
    query7 = '''SELECT channel_name AS channelname, views AS totalviews FROM channels'''
    cursor.execute(query7)
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["Channel Name", "Total Views"])
    st.write(df7)

elif question == "8. Videos published in the year 2022":
    query8 = '''SELECT title AS video_title, published_date AS videorelease, channel_name AS channelname FROM videos WHERE EXTRACT(YEAR FROM published_date)=2022'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["Video Title", "Published Date", "Channel Name"])
    st.write(df8)

elif question == "9. Average duration of all videos in each channel":
    query9 = '''SELECT channel_name AS channelname, AVG(duration) AS averageduration FROM videos GROUP BY channel_name'''
    cursor.execute(query9)
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["Channel Name", "Average Duration"])
    
    # Format the duration to be more readable
    df9['Average Duration'] = df9['Average Duration'].apply(lambda x: str(x))
    st.write(df9)

elif question == "10. Videos with the highest number of comments":
    query10 = '''SELECT title AS videotitle, channel_name AS channelname, comments AS comments FROM videos WHERE comments IS NOT NULL ORDER BY comments DESC'''
    cursor.execute(query10)
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["Video Title", "Channel Name", "Comments"])
    st.write(df10)
