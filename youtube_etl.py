import os
import pandas as pd
from googleapiclient.discovery import build
import json
from datetime import datetime
import s3fs 


def run_youtube_etl():
    # Set up your YouTube Data API v3 credentials
    api_key = 'API-KEY'
    youtube = build('youtube', 'v3', developerKey=api_key)

    # Specify the video ID for which you want to fetch comments
    video_id = 'zhEWqfP6V_w'

    # Fetch comments using the API
    def get_video_comments(youtube, **kwargs):
        comments = []
        results = youtube.commentThreads().list(**kwargs).execute()

        while results:
            for item in results['items']:
                comment_data = {
                    'VideoID': video_id,  # Adding the VideoID to the comment data
                    'Comment': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'AuthorChannelURL': f"https://www.youtube.com/channel/{item['snippet']['topLevelComment']['snippet']['authorChannelId']['value']}",
                    'CommentDate': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                    'UpdatedAt': item['snippet']['topLevelComment']['snippet']['updatedAt'],
                    'LikeCount': item['snippet']['topLevelComment']['snippet']['likeCount']
                }
                comments.append(comment_data)

            # Check if there are more comments
            if 'nextPageToken' in results:
                kwargs['pageToken'] = results['nextPageToken']
                results = youtube.commentThreads().list(**kwargs).execute()
            else:
                break

        return comments

    video_comments = get_video_comments(
        youtube, part='snippet', videoId=video_id, textFormat='plainText')

    # Convert comments to a DataFrame
    comments_df = pd.DataFrame(video_comments)

    # Save comments DataFrame to CSV
    comments_csv_filename = 'video_comments.csv'
    comments_df.to_csv(comments_csv_filename, index=False)

    print(f"Comments data saved to {comments_csv_filename}")

    # Fetch video information using the API
    def get_video_information(youtube, video_id):
        video_info = youtube.videos().list(part='snippet', id=video_id).execute()
        return video_info

    video_info = get_video_information(youtube, video_id)

    # Extract video title and description
    title = video_info['items'][0]['snippet']['title']
    description = video_info['items'][0]['snippet']['description']

    # Create a DataFrame for video information
    video_info_df = pd.DataFrame({
        'VideoID': [video_id],
        'Title': [title],
        'Description': [description]
    })

    # Save video information DataFrame to CSV
    video_info_csv_filename = 'video_info.csv'
    video_info_df.to_csv(video_info_csv_filename, index=False)

    print(f"Video information data saved to {video_info_csv_filename}")
