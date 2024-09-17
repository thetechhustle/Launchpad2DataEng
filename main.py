import os
from dotenv import load_dotenv
import requests
import csv

# Main script for data engineering project

def fetch_data():
    """
    This function fetches data from the Reddit API.
    It authenticates using OAuth2, retrieves an access token, and fetches data from a subreddit.
    """
    # Load environment variables from .env file
    load_dotenv()

    client_id = os.getenv('REDDIT_CLIENT_ID')
    client_secret = os.getenv('REDDIT_CLIENT_SECRET')
    redirect_uri = os.getenv('REDDIT_REDIRECT_URI')
    username = os.getenv('REDDIT_USERNAME')  # Optional
    password = os.getenv('REDDIT_PASSWORD')  # Optional

    # OAuth2 authentication
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)

    # Data needed for the POST request to get the access token
    data = {
        'grant_type': 'client_credentials'
    }

    headers = {
        'User-Agent': 'DataStream Fetcher/0.1 by Such-Bicycle-8652'
    }

    # Make a POST request to get the access token
    response = requests.post('https://www.reddit.com/api/v1/access_token',
                             auth=auth, data=data, headers=headers)

    # Check if the response is successful
    if response.status_code == 200:
        token = response.json().get('access_token')
        print("Access token received!")

        # Use the access token to fetch data from a subreddit
        headers = {
            'Authorization': f'bearer {token}',
            'User-Agent': 'DataStream Fetcher/0.1 by Such-Bicycle-8652'
        }

        # Fetch the latest posts from the r/[enter subreddit] with a limit of 100 posts
        subreddit_response = requests.get('https://oauth.reddit.com/r/investing/new?limit=100', headers=headers)

        # Check if the response is successful
        if subreddit_response.status_code == 200:
            print("Successfully fetched data from subreddit!")
            # Process the fetched data
            data = subreddit_response.json()

            # Open a CSV file for writing
            with open('reddit_investing_posts.csv', mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                # Write the header row
                writer.writerow(['Title', 'Author', 'Score', 'URL', 'Created_UTC', 'Content'])

                # Write the data rows
                for post in data['data']['children']:
                    post_data = post['data']
                    writer.writerow([
                        post_data['title'],
                        post_data['author'],
                        post_data['score'],
                        post_data['url'],
                        post_data['created_utc'],
                        post_data.get('selftext', '')  # Extract the post content
                    ])

            print("Data has been written to reddit_investing_posts.csv")

        else:
            print(f"Failed to fetch data from subreddit. Status code: {subreddit_response.status_code}")
    else:
        print(f"Failed to get access token. Status code: {response.status_code}")
        print(f"Response content: {response.content}")

if __name__ == "__main__":
    fetch_data()