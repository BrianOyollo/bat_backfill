import os
import time
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
import boto3
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

S3_BUCKET = os.getenv("S3_BUCKET")
NTFY_TOPIC = os.getenv("NTFY_TOPIC")

def send_notification(topic, message):
    requests.post(
        f"https://ntfy.sh/{topic}",
        data=f"{message}".encode(encoding='utf-8')
    )


def upload_to_s3(s3_client, auction_data:list[dict]):

    """ Uploads auction data to an S3 bucket in Parquet format.
    """

    buffer = io.BytesIO()
    df = pd.DataFrame(auction_data)
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
    buffer.seek(0)

    timestamp = int(time.time())

    try:
        s3_client.put_object(
            Bucket = S3_BUCKET,
            Key = f'auctions_urls/{timestamp}.parquet',
            Body = buffer.getvalue()
        )
        
    except Exception as e:
        send_notification(NTFY_TOPIC,"Error uploading file to s3")
        raise
        


def fetch_auctions(page: int) -> dict|int:
    """Fetch auctions for a single page and return data as list of dicts."""
    print(f"Fetching page {page}...")
    try:
        url = f"https://bringatrailer.com/wp-json/bringatrailer/1.0/data/listings-filter?page={page}&per_page=60&get_items=1&get_stats=0&sort=td"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        print(f"Fetching page {page}...done!")
        
        snapshot_data = {
            'items_per_page': data['items_per_page'],
            'items_total': data['items_total'],
            'last_read_page': data['page_current'],
            'pages_total': data['pages_total'],
            'snapshot_time': datetime.now().isoformat()
        }

        auctions_data = data['items']
        for item in auctions_data:
            item['fetched_at'] = datetime.now().isoformat()
            
        return {
            "data": data,
            'snapshot_data': snapshot_data
        }
    except Exception as e:
        print(f"Error fetching page {page}: {e}")
        return page
    
def save_snapshot(snapshot_data:dict, header:bool = False):
    df = pd.DataFrame([snapshot_data])
    df.to_csv("snapshot.csv", mode='a', header=header, index=False)

def read_last_saved_snapshot():
    df = pd.read_csv("snapshot.csv")
    df = df.sort_values('snapshot_time')
    latest = df.iloc[-1].to_dict() 
    return latest



def backfill(s3_client):

    step = 15
    max_workers = 15
    failed_pages = []

    print(">>> Fetching current auction stats...")
    current_snapshot = fetch_auctions(1)
    time.sleep(100)

    if not isinstance(current_snapshot, dict):
        return
    
    current_snapshot = current_snapshot['snapshot_data']
    total_pages = current_snapshot['pages_total']

    if os.path.exists('snapshot.csv'):
        print(">>> Fetching most recently saved auction stats...")
        last_saved_snapshot = read_last_saved_snapshot()
        most_recent_snapshot = last_saved_snapshot
    
        new_auctions = current_snapshot['items_total'] - last_saved_snapshot['items_total']

        if new_auctions <= 0:
            start_page = last_saved_snapshot['last_read_page']+1
        else:
            delta_pages = new_auctions // 60
            start_page = last_saved_snapshot['last_read_page']+delta_pages+1
    else:
        print(">>> Using current stats as the first snapshot...")
        start_page = current_snapshot['last_read_page']
        most_recent_snapshot = current_snapshot


    batch = []
    

    print(">>> Fetchign auction urls...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        pages_batch = [i for i in range(start_page, min(start_page+step, total_pages+1))]
        futures = [executor.submit(fetch_auctions, page) for page in pages_batch]
        for future in as_completed(futures):
            result = future.result()

            batch.append(result['data'])

            if result['snapshot_data']['last_read_page'] > most_recent_snapshot['last_read_page']:
                most_recent_snapshot = result['snapshot_data']

    # save snapshot
    print(">>> Updating snapshot file...")
    if os.path.exists('snapshot.csv'):
        save_snapshot(result['snapshot_data'], header=False)
    else:
        save_snapshot(result['snapshot_data'], header=True)


    # upload file to s3
    print(">>> Uploading auction file to s3...")
    upload_to_s3(s3_client, batch)

    


if __name__ == '__main__':
    s3_client = boto3.client(
        's3',
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name = os.getenv("AWS_REGION")
    )

    backfill(s3_client)