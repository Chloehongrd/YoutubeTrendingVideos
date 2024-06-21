import os
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import bigquery
from datetime import datetime, timezone
import isodate # Convert video duration to seconds 
import time # Record Runtime 
from concurrent.futures import ThreadPoolExecutor,as_completed
from flask import Request

# Configure logging
logging.basicConfig(level=logging.INFO)

def fetch_trending_videos(request: Request):
    """
    Fetches trending videos by category and inserts them into BigQuery.

    This function retrieves the list of video categories available in a specified region,
    fetches the trending videos for each category using the YouTube Data API, and inserts
    the video data into a BigQuery table. It uses a ThreadPoolExecutor to handle multiple
    categories concurrently for improved performance.

    Args:
        request (Request): The request object containing the HTTP request data.

    Returns:
        tuple: A success message and HTTP status code.
    """
    start_time = time.time()
    
    # Retrieve environment variables
    api_key = os.environ.get('API_KEY')
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')
    region_code = 'US'
    logging.info("Starting the process...")

    try:
        # List video categories
        categories = list_video_categories(api_key, region_code)
    # Handle API errors
    except HttpError as e:
        if e.resp.status == 403 and 'insufficientPermissions' in e.content.decode():
            logging.error("Insufficient permissions to access video categories. Check your API key and permissions.")
            return 'Error'
        else:
            logging.error(f"An error occurred: {e}")
            return 'Error'
    
    # Use ThreadPoolExecutor to handle multiple categories concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for category in categories:
            category_id = category['id']
            futures.append(executor.submit(fetch_and_insert_videos, api_key, project_id, dataset_id, category_id))
        
        for future in futures:
            future.result()

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f'Task completed in {duration} seconds')
    
    return 'Success', 200

def list_video_categories(api_key, region_code='US'):
    """
    Lists video categories available in the specified region.
    """
    logging.info(f"Listing video categories for {region_code}")
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.videoCategories().list(
        part="snippet",
        regionCode=region_code
    )
    response = request.execute()
    return response['items']

def get_trending_videos(api_key, video_category_id, max_results=50):
    """
    Retrieves trending videos for a given category.
    """
    logging.info(f"Getting trending videos for category {video_category_id}...")
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.videos().list(
        part="snippet,statistics,contentDetails,status,topicDetails",
        chart="mostPopular",
        videoCategoryId=video_category_id,
        maxResults=max_results
    )
    response = request.execute()
    videos = response.get('items', [])
    for video in videos:
        video['video_url'] = f"https://www.youtube.com/watch?v={video['id']}"
    return videos

def fetch_and_insert_videos(api_key, project_id, dataset_id, category_id):
    """
    Fetches trending videos and inserts them into the BigQuery table.
    """
    try:
        trending_videos = get_trending_videos(api_key, category_id, max_results=50)
        logging.info(f"Fetched {len(trending_videos)} videos for category {category_id}")

        if trending_videos:
            insert_into_top_videos_table(project_id, dataset_id, trending_videos)
        else:
            logging.warning(f"No trending videos found for category {category_id}.")
    except HttpError as e:
        if e.resp.status == 400 and 'videoChartNotFound' in e.content.decode():
            logging.warning(f"Skipping category {category_id} as it doesn't support mostPopular chart.")
        else:
            logging.error(f"Error fetching trending videos for category {category_id}: {e}")

def insert_into_top_videos_table(project_id, dataset_id, videos):
    """
    Inserts trending videos into the BigQuery table.
    """
    logging.info("Inserting top videos into BigQuery table...")
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.trending_vids"
    rows_to_insert = []

    # handle duplicates if the function is run multiple times in a day 
    existing_ids = get_existing_video_ids(project_id, dataset_id)
    logging.info(f"Fetched {len(existing_ids)} existing video IDs for today")
    
    for video in videos:
        if video['id'] in existing_ids:
            logging.info(f"Skipping duplicate video {video['id']}")
            continue

        duration_seconds = parse_duration_to_seconds(video['contentDetails']['duration'])

        row = {
            'id': video['id'],
            'date_extracted': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'title': video['snippet']['title'],
            'duration_seconds': duration_seconds,
            'thumbnails_standard': video['snippet']['thumbnails']['standard']['url'],
            'published_at': video['snippet']['publishedAt'],
            'made_for_kids': video['status']['madeForKids'],
            'content_rating_mpaa_rating': video.get('contentRating', {}).get('mpaaRating', None),
            'channel_id': video['snippet']['channelId'],
            'channel_title': video['snippet']['channelTitle'],
            'category_id': int(video['snippet']['categoryId']),
            'video_url': video['video_url'],  # Include the video URL
            'live_broadcast_content': video['snippet']['liveBroadcastContent'],
            'default_audio_language': video['snippet'].get('defaultAudioLanguage', None)
        }

        rows_to_insert.append(row)
    
    if rows_to_insert:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors == []:
            logging.info('Data successfully uploaded to BigQuery')
        else:
            logging.error(f'Errors occurred: {errors}')
    else:
        logging.warning("No new videos to upload.")

def get_existing_video_ids(project_id, dataset_id):
    """
    Fetches today's existing video IDs from BigQuery to avoid duplicates.
    """
    logging.info("Fetching today's existing video IDs from BigQuery...")
    client = bigquery.Client(project=project_id)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    query = f"""
        SELECT id, date_extracted FROM `{project_id}.{dataset_id}.trending_vids`
        WHERE date_extracted = '{today}'
    """
    query_job = client.query(query)
    results = query_job.result()
    return {(row.id, row.date_extracted) for row in results}

def parse_duration_to_seconds(duration_str):
    """
    Parses ISO 8601 duration string to total seconds.
    """
    logging.info(f"Parsing duration {duration_str} to seconds...")
    duration = isodate.parse_duration(duration_str)
    return int(duration.total_seconds())

#-------------------------------------------
#--fetch_video_stats------------------------
#-------------------------------------------
def fetch_video_stats(request: Request):
    """
    Tracks engagement metrics of trending videos for up to 30 days.

    This function initiates the process of fetching and updating video statistics for
    trending YouTube videos. It retrieves video IDs to track from BigQuery, fetches
    their engagement metrics using the YouTube Data API, and uploads the updated metrics
    to BigQuery. The process is logged and the total duration is recorded. 
    Each video stats are tracked for 30 days

    Args:
        request (Request): The request object containing the HTTP request data.

    Returns:
        tuple: A success message and HTTP status code.
    """
    start_time = time.time()
    
    api_key = os.environ.get('API_KEY')
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')

    logging.info("Starting the process...")

    video_ids = get_video_ids_to_track(project_id, dataset_id)

    track_engagement_metrics(api_key, project_id, dataset_id, video_ids)
    
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f'Task completed in {duration} seconds')
    
    return 'Success', 200

def get_video_ids_to_track(project_id, dataset_id):
    """
    Fetches video IDs from BigQuery.
    """
    logging.info("Fetching video IDs to track from BigQuery...")
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT id, MIN(date_extracted) as earliest_date_extracted 
        FROM `{project_id}.{dataset_id}.trending_vids`
        GROUP BY id
    """
    query_job = client.query(query)
    results = query_job.result()

    current_date = datetime.now(timezone.utc).date()

    video_ids_to_track = []
    for row in results:
        video_id = row.id
        earliest_date_extracted = row.earliest_date_extracted

        days_since_extracted = (current_date - earliest_date_extracted).days

        if 0 <= days_since_extracted < 30:
            video_ids_to_track.append(video_id)

    logging.info(f"Found {len(video_ids_to_track)} video IDs to track.")
    return video_ids_to_track

def fetch_engagement_metrics(api_key, video_id):
    """
    Fetches engagement metrics for a given video.
    """
    logging.info(f"Fetching engagement metrics for video {video_id}...")
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.videos().list(
        part='statistics',
        id=video_id
    )
    response = request.execute()
    logging.info(f"Response from YouTube API for video {video_id}: {response}")
    
    if 'items' in response and response['items']:
        stats = response['items'][0]['statistics']
        logging.info(f"Fetched metrics for video {video_id}: {stats}")
        return {
            'view_count': int(stats.get('viewCount', 0)),
            'like_count': int(stats.get('likeCount', 0)),
            'comment_count': int(stats.get('commentCount', 0)),
        }
    logging.warning(f"No metrics found for video {video_id}.")
    return {}


def track_engagement_metrics(api_key, project_id, dataset_id, video_ids):
    """
    Tracks engagement metrics for a list of video IDs.
    """
    logging.info("Tracking engagement metrics...")
    updated_metrics = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_video_id = {executor.submit(fetch_engagement_metrics, api_key, video_id): video_id for video_id in video_ids}
        
        for future in as_completed(future_to_video_id):
            video_id = future_to_video_id[future]
            try:
                metrics = future.result()
                if metrics:
                    updated_metrics.append({
                        'id': video_id,
                        'date_extracted': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                        'view_count': metrics['view_count'],
                        'like_count': metrics['like_count'],
                        'comment_count': metrics['comment_count']
                    })
            except Exception as exc:
                logging.error(f'Video ID {video_id} generated an exception: {exc}')
    
    if updated_metrics:
        upload_to_bigquery(updated_metrics, project_id, dataset_id, 'vids_stats')
    else:
        logging.warning("No engagement metrics to upload.")

def upload_to_bigquery(data, project_id, dataset_id, table_name):
    logging.info(f"Uploading data to BigQuery table {table_name}...")
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    errors = client.insert_rows_json(table_id, data)
    if errors == []:
        logging.info('Data successfully uploaded to BigQuery')
    else:
        logging.error(f'Errors occurred: {errors}')


#---------------------------------------------
#--fetch_channel_stats------------------------
#---------------------------------------------

def fetch_channel_stats(request: Request):
    """
    Fetches and uploads channel statistics to BigQuery.

    This function retrieves the list of YouTube channel IDs from BigQuery trending_vids table,
    fetches the channel statistics using the YouTube Data API, and uploads the data to 
    BigQuery for analysis. It runs multiple fetch operations concurrently to improve 
    performance. Each channel is tracked for 1 year 

    Args:
        request (Request): The request object containing HTTP request data.

    Returns:
        tuple: A success message and HTTP status code.
    """
    start_time = time.time()
    
    api_key = os.environ.get('API_KEY')
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')

    logging.info("Starting the process...")

    channel_ids = get_channel_ids_to_track(project_id, dataset_id)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_and_upload_channel_stats, api_key, project_id, dataset_id, channel_id) for channel_id in channel_ids]
        
        for future in futures:
            future.result()

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f'Task completed in {duration} seconds')

    return 'Success', 200

def get_channel_ids_to_track(project_id, dataset_id):
    """
    Retrieves the list of YouTube channel IDs.
    """
    logging.info("Fetching channel IDs to track from BigQuery...")
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT channel_id, MIN(date_extracted) as earliest_date_extracted 
        FROM `{project_id}.{dataset_id}.trending_vids`
        GROUP BY channel_id
    """
    query_job = client.query(query)
    results = query_job.result()

    # Get the current date
    current_date = datetime.now(timezone.utc).date()

    # Filter channels that need tracking
    channel_ids_to_track = []
    for row in results:
        channel_id = row.channel_id
        earliest_date_extracted = row.earliest_date_extracted

        # Calculate the difference in days between today and the earliest extraction date
        days_since_extracted = (current_date - earliest_date_extracted).days

        # If the channel was extracted within the last 365 days, track its metrics
        if 0 <= days_since_extracted < 365:
            channel_ids_to_track.append(channel_id)

    logging.info(f"Found {len(channel_ids_to_track)} channel IDs to track.")
    return channel_ids_to_track

def fetch_and_upload_channel_stats(api_key, project_id, dataset_id, channel_id):
    """
    Fetches and uploads channel statistics for a given channel ID.
    """
    logging.info(f"Fetching and uploading channel statistics for {channel_id}...")
    
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics,topicDetails,status,brandingSettings",
            id=channel_id
        )
        response = execute_request_with_retries(request)
        if 'items' in response and response['items']:
            channel = response['items'][0]

            topic_ids = ','.join(channel.get('topicDetails', {}).get('topicIds', []))
            topic_categories = ','.join([cat.split('/')[-1] for cat in channel.get('topicDetails', {}).get('topicCategories', [])])

            row = {
                'channel_id': channel['id'],
                'title': channel['snippet']['title'],
                'customUrl': channel['snippet'].get('customUrl', ''),
                'publishedAt': channel['snippet']['publishedAt'],
                'extracted_date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),

                'country': channel['snippet'].get('country', ''),
                'uploads': channel['contentDetails']['relatedPlaylists']['uploads'],
                'viewCount': int(channel['statistics'].get('viewCount', 0)),
                'commentCount': int(channel['statistics'].get('commentCount', 0)),
                'subscriberCount': int(channel['statistics'].get('subscriberCount', 0)),
                'hiddenSubscriberCount': channel['statistics'].get('hiddenSubscriberCount', False),
                'videoCount': int(channel['statistics'].get('videoCount', 0)),
                'topicIds': topic_ids,
                'topicCategories': topic_categories,

                'keywords': channel['brandingSettings']['channel'].get('keywords', ''),
                'longUploadsStatus': channel['status'].get('longUploadsStatus', ''),
                'privacyStatus': channel['status'].get('privacyStatus', ''),
                'isLinked': channel['status'].get('isLinked', False),
                'madeForKids': channel['status'].get('madeForKids', False),
                'channel_url': f"https://www.youtube.com/channel/{channel_id}"
            }

            logging.info(f"Prepared row for channel {channel_id}: {row}")
            upload_to_bigquery([row], project_id, dataset_id, 'channel_stats')
        else:
            logging.warning(f"No data found for channel {channel_id}.")
    except HttpError as e:
        logging.error(f"Error fetching channel statistics for {channel_id}: {e}")

def execute_request_with_retries(request, retries=5, delay=1):
    """
    Executes a request with retries on temporary server errors.
    """
    for _ in range(retries):
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status in [500, 502, 503, 504]:
                logging.warning(f"Temporary server error ({e.resp.status}). Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    raise Exception("Max retries exceeded")

def upload_to_bigquery(data, project_id, dataset_id, table_name):
    logging.info(f"Uploading data to BigQuery table {table_name}...")
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    errors = client.insert_rows_json(table_id, data)
    if errors == []:
        logging.info('Data successfully uploaded to BigQuery')
    else:
        logging.error(f'Errors occurred: {errors}')

