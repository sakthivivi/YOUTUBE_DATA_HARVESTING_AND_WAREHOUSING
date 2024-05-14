from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import streamlit as st

key=build("youtube","v3",developerKey="AIzaSyD6AE6f433MOgdjminuvEFBg-T8XFVlZPM")

global details
details=[]

global channel_df
channel_df  = pd.DataFrame()
global playlists_df
playlists_df = pd.DataFrame()
global videos_df
videos_df = pd.DataFrame()
global comments_df
comments_df = pd.DataFrame()

def get_channel_info(channel_id):
    input=key.channels().list(
             part="snippet,ContentDetails,statistics",
             id=channel_id
    )
    output=input.execute()
    print(output)

    for i in output['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                 Channel_Id=i["id"],
                 Subscribers=i["statistics"]["subscriberCount"],
                 Views=i["statistics"]["viewCount"],
                 Total_Video=i["statistics"]["videoCount"],
                 Channel_Description=i["snippet"]["description"],
                 Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

def get_videos_ids(channel_id):
    video_ids=[]
    respect=key.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    playlist_Id=respect["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token=None
    while True:
        respect1=key.playlistItems().list(
                                    part="snippet",
                                    playlistId=playlist_Id,
                                    maxResults=50,
                                    pageToken=next_page_token).execute()
        for i in range(len(respect1["items"])):
            video_ids.append(respect1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token=respect1.get("nextPageToken")
        if next_page_token is None:
            break
    return video_ids

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        input=key.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        output=input.execute()
        for i in output["items"]:
            data=dict(Channel_Name=i["snippet"]["channelTitle"],
                    Channel_Id=i["snippet"]["channelId"],
                    Video_Id=i["id"],
                    Title=i["snippet"]["title"],
                    Tags=i["snippet"].get("tags"),
                    Thumbnail=i["snippet"]["thumbnails"]["default"]["url"],
                    Description=i["snippet"].get("description"),
                    Published_Date=i["snippet"]["publishedAt"],
                    Duration=i["contentDetails"]["duration"],
                    Views=i["statistics"].get("viewCount"),
                    Likes=i["statistics"].get("likeCount"),
                    Dislikes=i["statistics"].get("dislikeCount"),
                    Comments=i["statistics"].get("commentCount"),
                    Favorite_Count=i["statistics"]["favoriteCount"],
                    Definition=i["contentDetails"]["definition"],
                    Caption_Status=i["contentDetails"]["caption"])
        
            video_data.append(data)      
    return video_data             


def get_comment_info(video_ids):
    Comment_data=[]
    for video_id in video_ids:
        input=key.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50
        )
        output=input.execute()
        for i in output["items"]:
            data=dict(Comment_Id=i["snippet"]["topLevelComment"]["id"],
                    Video_Id=i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                    Comment_Text=i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    Comment_Author=i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    Comment_Published=i["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
            
            Comment_data.append(data)
    return Comment_data


def get_playlist_details(channel_id):
    next_page_token=None
    Play_data=[]
    while True:
            input=key.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
            )
            output=input.execute()
            for i in output["items"]:
                    data=dict(Playlist_Id=i["id"],
                            Title=i["snippet"]["title"],
                            Channal_Id=i["snippet"]["channelId"],
                            Channal_Name=i["snippet"]["channelTitle"],
                            PublishedAt=i["snippet"]["publishedAt"],
                            Video_Count=i["contentDetails"]["itemCount"])
                    Play_data.append(data)
            next_page_token=output.get("nextPageToken")
            if next_page_token is None:
                    break
    return Play_data        

def channel_details(channel_ids):
    all_channel_details = []
    for channel_id in channel_ids:
        channel_info = get_channel_info(channel_id)
        playlist_info = get_playlist_details(channel_id)
        video_ids = get_videos_ids(channel_id)
        video_info = get_video_info(video_ids)
        comment_info = get_comment_info(video_ids)
        channel_details = {
             "channel_id": channel_id,
            "channel_info": channel_info,
            "playlist_info": playlist_info,
            "video_info": video_info,
            "comment_info": comment_info
        }
        all_channel_details.append(channel_details)
    return all_channel_details

channel_ids=["UCcUPukReK4lbLGx8nv5r2oQ"]
details=channel_details(channel_ids)



def create_channels():
    global channel_df
    myconnection = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="VIVISHA",
            database="youtube",
            port="5432"
            )
    cursor = myconnection.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    myconnection.commit()


    create_table_query = '''
    CREATE TABLE IF NOT EXISTS channels (
                    Channel_Name VARCHAR(100),
                    Channel_Id VARCHAR(80) PRIMARY KEY,
                    Subscribers BIGINT,
                    Views BIGINT,
                    Total_Videos INT,
                    Channel_Description TEXT,
                    Playlist_Id VARCHAR(80)
            )
            '''
    cursor.execute(create_table_query)
    myconnection.commit()


    data=[]
    global details
    for detail in details:
            data.append(detail["channel_info"])
    if channel_df is None:
        channel_df = pd.DataFrame(data)
    else:
        channel_df = pd.concat([channel_df, pd.DataFrame(data)], ignore_index=True)
    #channel_df=pd.DataFrame(data)


    for index,row in channel_df.iterrows():
            insert_values='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Views'],
                    row['Total_Video'],
                    row['Channel_Description'],
                    row['Playlist_Id'])
            
            cursor.execute(insert_values,values)
            myconnection.commit()


def create_playlists():
    global playlists_df
    myconnection = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="VIVISHA",
            database="youtube",
            port="5432"
            )
    cursor = myconnection.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    myconnection.commit()

    create_table_query = '''
    CREATE TABLE IF NOT EXISTS playlists (
            Playlist_Id VARCHAR(100) PRIMARY KEY,
            Title VARCHAR(100),
            Channel_Id VARCHAR(100),
            Channel_Name VARCHAR(100),
            PublishedAt TIMESTAMP,
            Video_Count INT
            )
            '''
    cursor.execute(create_table_query)
    myconnection.commit()


    playlist_data=[]
    global details
    for detail in details:
        for i in range(len(detail["playlist_info"])):
            playlist_data.append(detail["playlist_info"][i])
    if playlists_df is None:
        playlists_df = pd.DataFrame(playlist_data)
    else:
        playlists_df = pd.concat([playlists_df, pd.DataFrame(playlist_data)], ignore_index=True)
    #df1=pd.DataFrame(playlist_data)

    for index,row in playlists_df.iterrows():
            insert_values='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt ,
                                            Video_Count)

                                            values(%s,%s,%s,%s,%s,%s)'''
            values=(row['Playlist_Id'],
                    row['Title'],
                    row['Channal_Id'],
                    row['Channal_Name'],
                    row['PublishedAt'],
                    row['Video_Count'])

            cursor.execute(insert_values,values)
            myconnection.commit()


def create_videos():
    global videos_df
    myconnection = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="VIVISHA",
            database="youtube",
            port="5432"
        )
    cursor = myconnection.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    myconnection.commit()
    
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS videos (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Video_Id VARCHAR(50) primary key,
            Title VARCHAR(150),
            Tags TEXT,
            Thumbnail VARCHAR(150),
            Description TEXT,
            Published_Date TIMESTAMP,
            Duration interval,
            Views BIGINT,
            Likes BIGINT,
            Dislikes BIGINT,
            Comments INT,
            Favorite_Count INT,
            Definition VARCHAR(30),
            Caption_Status VARCHAR(50)
        )
        '''
    cursor.execute(create_table_query)
    myconnection.commit()


    video_data=[]
    global details
    for detail in details:
        for i in range(len(detail["video_info"])):
          video_data.append(detail["video_info"][i])
    if videos_df is None:
        videos_df = pd.DataFrame(video_data)
    else:
        videos_df = pd.concat([videos_df, pd.DataFrame(video_data)], ignore_index=True)     
    #df2=pd.DataFrame(video_data)


    for index,row in videos_df.iterrows():
        insert_values='''insert into videos(Channel_Name,
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
                                            Dislikes, 
                                            Comments, 
                                            Favorite_Count, 
                                            Definition,
                                            Caption_Status) 

                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
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
                row['Dislikes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status'])
        
        cursor.execute(insert_values,values)
        myconnection.commit()


def create_comments():
    global comments_df
    myconnection = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="VIVISHA",
            database="youtube",
            port="5432"
        )
    cursor = myconnection.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    myconnection.commit()

    
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS comments (
                        Comment_Id VARCHAR(100) PRIMARY KEY,
                        Video_Id VARCHAR(50),
                        Comment_Text TEXT,
                        Comment_Author VARCHAR(150),
                        Comment_Published TIMESTAMP
                )
                '''
    cursor.execute(create_table_query)
    myconnection.commit()

    
    comment_data=[]
    global details
    for detail in details:
        for i in range(len(detail["comment_info"])):
            comment_data.append(detail["comment_info"][i])
    if comments_df is None:
        comments_df = pd.DataFrame(comment_data)
    else:
        comments_df = pd.concat([comments_df, pd.DataFrame(comment_data)], ignore_index=True) 
    #df3=pd.DataFrame(comment_data)

    for index, row in comments_df.iterrows():
        insert_values='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published)
                        
                                                values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published'])

        cursor.execute(insert_values,values)
        myconnection.commit()

def migrate_to_sql():   
    create_channels()
    create_playlists()
    create_videos()
    create_comments()
    return "Data migrated to SQL successfully!"

show_me=migrate_to_sql()


def view_channels():
    global channel_df
    #data = []
    #global details
    #for detail in details:
    #    data.append(detail["channel_info"])
    #df=pd.DataFrame(data)
    st.write(channel_df)

def view_playlists():
    global playlists_df
    #playlist_data=[]
    #global details
    #for detail in details:
    #       for i in range(len(detail["playlist_info"])):
    #           playlist_data.append(detail["playlist_info"][i])
    #df1=pd.DataFrame(playlist_data)
    st.write(playlists_df)

def view_videos():
    global videos_df
    #video_data=[]
    #global details
    #for detail in details:
    #    for i in range(len(detail["video_info"])):
    #       video_data.append(detail["video_info"][i])
    #df2=pd.DataFrame(video_data)
    st.write(videos_df)

def view_comments():
    global comments_df
    #comment_data=[]
    #global details
    #for detail in details:
    #   for i in range(len(detail["comment_info"])):
    #           comment_data.append(detail["comment_info"][i])
    #df3=pd.DataFrame(comment_data)
    st.write(comments_df)

st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
st.subheader("using SQL and Streamlit")
st.header("Skills take away From This Project")
st.markdown("- **Python Scripting**: Master the art of automating tasks and manipulating data using Python.")
st.markdown("- **Data Collection**: Learn how to gather information efficiently from various sources.")
st.markdown("- **API Integration**: Explore ways to connect and interact with APIs to access valuable data.")
st.markdown("- **Data Management using SQL**: Acquire skills to organize, store, and query data using SQL databases.")
st.markdown("- **Streamlit**: Create web applications for machine learning, data analysis, and visualization")

channel_ids = st.text_input("Enter Channel IDs (comma-separated):")
channel_ids = channel_ids.split(",") if channel_ids else []

if st.button("Collect Data"):
    details=channel_details(channel_ids)
    show_me = migrate_to_sql()
    st.success("Data collected and migrated to SQL successfully!")

data_type = st.selectbox("Select Data Type:", ["Channels","Playlists","Videos", "Comments"])

if data_type == "Channels":
    view_channels()
elif data_type == "Playlists":
    view_playlists()     
elif data_type == "Videos":
    view_videos()
elif data_type == "Comments":
    view_comments()

    
def execute_query(query):
    connection = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="VIVISHA",
        database="youtube",
        port="5432"
    )
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    connection.close()
    return results

def get_videos_and_channels():
    query = "SELECT Title, Channel_Name FROM videos"
    return execute_query(query)

def channels_with_most_videos():
    query = "SELECT Channel_Name, COUNT(*) AS Video_Count FROM videos GROUP BY Channel_Name ORDER BY Video_Count DESC LIMIT 5"
    return execute_query(query)

def top_10_viewed_videos():
    query = "SELECT Title, Channel_Name, Views FROM videos ORDER BY Views DESC LIMIT 10"
    return execute_query(query)

def comments_per_video():
    query = "SELECT Comments AS No_Comments, Title AS Video_Title from videos WHERE comments is Not Null"
    return execute_query(query)

def videos_with_most_likes():
    query = "SELECT Title, Channel_Name, Likes FROM videos ORDER BY Likes DESC LIMIT 5"
    return execute_query(query)

def total_likes_dislikes_per_video():
    query = "SELECT Title, SUM(Likes), SUM(Dislikes) FROM videos GROUP BY Title"
    return execute_query(query)

def total_views_per_channel():
    query = "SELECT Channel_Name, SUM(Views) AS Total_Views FROM videos GROUP BY Channel_Name"
    return execute_query(query)

def channels_with_videos_2022():
    query = "SELECT DISTINCT Channel_Name FROM videos WHERE EXTRACT(YEAR FROM Published_Date) = 2022"
    return execute_query(query)

def average_duration_per_channel():
    query = "SELECT Channel_Name, AVG(Duration) AS Average_Duration FROM videos GROUP BY Channel_Name"
    return execute_query(query)

def videos_with_most_comments():
    query = "SELECT Title, Channel_Name, Comments FROM Videos WHERE Comments is Not Null ORDER BY Comments DESC"
    return execute_query(query)


selected_question = st.selectbox("Select your question", [
    "What are the names of all the videos and their corresponding channels?",
    "Which channels have the most number of videos, and how many videos do they have?",
    "What are the top 10 most viewed videos and their respective channels?",
    "How many comments were made on each video, and what are their corresponding video names?",
    "Which videos have the highest number of likes, and what are their corresponding channel names?",
    "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "What is the total number of views for each channel, and what are their corresponding channel names?",
    "What are the names of all the channels that have published videos in the year 2022?",
    "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "Which videos have the highest number of comments, and what are their corresponding channel names?"])

if selected_question == "What are the names of all the videos and their corresponding channels?":
    results = get_videos_and_channels()
    column_names = ["Title", "Channel_Name"]
elif selected_question == "Which channels have the most number of videos, and how many videos do they have?":
    results = channels_with_most_videos()
    column_names = ["Channel_Name", "Video_Count"]
elif selected_question == "What are the top 10 most viewed videos and their respective channels?":
    results = top_10_viewed_videos()
    column_names = ["Title", "Channel_Name", "Views"]
elif selected_question == "How many comments were made on each video, and what are their corresponding video names?":
    results = comments_per_video()
    column_names = ["Comment_Count","Title"]
elif selected_question == "Which videos have the highest number of likes, and what are their corresponding channel names?":
    results = videos_with_most_likes()
    column_names = ["Title", "Channel_Name", "Likes"]
elif selected_question == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    results = total_likes_dislikes_per_video()
    column_names = ["Title", "Total_Likes", "Total_Dislikes"]
elif selected_question == "What is the total number of views for each channel, and what are their corresponding channel names?":
    results = total_views_per_channel()
    column_names = ["Channel_Name", "Total_Views"]
elif selected_question == "What are the names of all the channels that have published videos in the year 2022?":
    results = channels_with_videos_2022()
    column_names = ["Channel_Name"]
elif selected_question == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    results = average_duration_per_channel()
    column_names = ["Channel_Name", "Average_Duration"]
elif selected_question == "Which videos have the highest number of comments, and what are their corresponding channel names?":
    results = videos_with_most_comments()
    column_names = ["Title", "Channel_Name", "Comment_Count"]

df = pd.DataFrame(results, columns=column_names)

st.write(df)


