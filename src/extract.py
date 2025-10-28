"""
YouTube data extraction using the Data API v3.
"""

import os
import time
from datetime import datetime
from typing import List, Dict
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import isodate

load_dotenv()


class YouTubeExtractor:
    """Handles video data collection from YouTube API."""
    
    CATEGORIES = {
        '10': 'Music',
        '20': 'Gaming',
        '22': 'People & Blogs',
        '24': 'Entertainment',
        '25': 'News & Politics',
        '27': 'Education',
        '28': 'Science & Technology'
    }
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('YOUTUBE_API_KEY')
        if not self.api_key:
            raise ValueError("YouTube API key not found")
        
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
    
    def search_videos(self, category_id: str, max_results: int = 50) -> List[str]:
        """Search for popular videos in a category."""
        try:
            request = self.youtube.videos().list(
                part='snippet',
                chart='mostPopular',
                videoCategoryId=category_id,
                maxResults=min(max_results, 50),
                regionCode='US'
            )
            response = request.execute()
            return [item['id'] for item in response.get('items', [])]
        except HttpError as e:
            print(f"Error searching category {category_id}: {e}")
            return []
    
    def get_video_details(self, video_ids: List[str]) -> List[Dict]:
        """Fetch detailed info for video IDs."""
        videos = []
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i+50]
            try:
                request = self.youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=','.join(batch)
                )
                response = request.execute()
                for item in response.get('items', []):
                    video_data = self._parse_video(item)
                    videos.append(video_data)
                time.sleep(0.1)
            except HttpError as e:
                print(f"Error fetching video details: {e}")
                continue
        return videos
    
    def get_channel_stats(self, channel_id: str) -> Dict:
        """Get channel subscriber count and stats."""
        try:
            request = self.youtube.channels().list(
                part='statistics',
                id=channel_id
            )
            response = request.execute()
            if response.get('items'):
                stats = response['items'][0]['statistics']
                return {
                    'subscriber_count': int(stats.get('subscriberCount', 0)),
                    'total_views': int(stats.get('viewCount', 0)),
                    'video_count': int(stats.get('videoCount', 0))
                }
        except HttpError as e:
            print(f"Error fetching channel stats: {e}")
        return {'subscriber_count': 0, 'total_views': 0, 'video_count': 0}
    
    def _parse_video(self, item: Dict) -> Dict:
        """Extract relevant fields from API response."""
        snippet = item.get('snippet', {})
        content = item.get('contentDetails', {})
        stats = item.get('statistics', {})
        duration_iso = content.get('duration', 'PT0S')
        duration_seconds = isodate.parse_duration(duration_iso).total_seconds()
        
        return {
            'video_id': item['id'],
            'title': snippet.get('title', ''),
            'channel_id': snippet.get('channelId', ''),
            'channel_title': snippet.get('channelTitle', ''),
            'category_id': snippet.get('categoryId', ''),
            'category_name': self.CATEGORIES.get(snippet.get('categoryId', ''), 'Unknown'),
            'published_at': snippet.get('publishedAt', ''),
            'duration_seconds': duration_seconds,
            'view_count': int(stats.get('viewCount', 0)),
            'like_count': int(stats.get('likeCount', 0)),
            'comment_count': int(stats.get('commentCount', 0)),
            'collected_at': datetime.now().isoformat()
        }
    
    def collect_by_categories(self, category_ids: List[str] = None, videos_per_category: int = 30) -> pd.DataFrame:
        """Main collection function."""
        if category_ids is None:
            category_ids = list(self.CATEGORIES.keys())
        
        all_videos = []
        for cat_id in category_ids:
            cat_name = self.CATEGORIES.get(cat_id, 'Unknown')
            print(f"Collecting {cat_name}...")
            video_ids = self.search_videos(cat_id, videos_per_category)
            if not video_ids:
                continue
            videos = self.get_video_details(video_ids)
            unique_channels = list(set([v['channel_id'] for v in videos]))
            channel_stats = {}
            for ch_id in unique_channels[:10]:
                stats = self.get_channel_stats(ch_id)
                channel_stats[ch_id] = stats
                time.sleep(0.1)
            for video in videos:
                ch_id = video['channel_id']
                if ch_id in channel_stats:
                    video.update(channel_stats[ch_id])
            all_videos.extend(videos)
            time.sleep(1)
        df = pd.DataFrame(all_videos)
        print(f"Collected {len(df)} videos")
        return df
    
    def save_data(self, df: pd.DataFrame, filename: str = None):
        """Save collected data to CSV."""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'youtube_data_{timestamp}.csv'
        filepath = os.path.join('data', 'raw', filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        df.to_csv(filepath, index=False)
        print(f"Saved to {filepath}")
        return filepath


if __name__ == '__main__':
    extractor = YouTubeExtractor()
    df = extractor.collect_by_categories(videos_per_category=30)
    extractor.save_data(df)
