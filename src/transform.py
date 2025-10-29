"""
Data processing and metrics calculation.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime


class DataTransformer:
    """Processes raw YouTube data and calculates ad metrics."""
    
    def __init__(self, filepath: str = None):
        self.filepath = filepath
        self.data = None
        self.processed = None
    
    def load_data(self, filepath: str = None):
        """Load raw data from CSV."""
        path = filepath or self.filepath
        if not path:
            raise ValueError("No filepath provided")
        
        self.data = pd.read_csv(path)
        print(f"Loaded {len(self.data)} records")
        return self.data
    
    def clean_data(self):
        """Basic data cleaning and validation."""
        df = self.data.copy()
        df = df.drop_duplicates(subset=['video_id'])
        numeric_cols = ['view_count', 'like_count', 'comment_count']
        df[numeric_cols] = df[numeric_cols].fillna(0)
        df = df[df['duration_seconds'] > 0]
        df['published_at'] = pd.to_datetime(df['published_at'])
        self.data = df
        return df
    
    def calculate_metrics(self):
        """Calculate ad-related metrics."""
        df = self.data.copy()
        
        # Duration in minutes
        df['duration_minutes'] = df['duration_seconds'] / 60
        
        # Base ad rates by category (industry estimates)
        category_rates = {
            'Music': 0.15,
            'Gaming': 0.25,
            'People & Blogs': 0.22,
            'Entertainment': 0.30,
            'News & Politics': 0.35,
            'Sports': 0.32,
            'Science & Technology': 0.22
        }
        df['base_ad_rate'] = df['category_name'].map(category_rates).fillna(0.25)
        
        df['channel_tier'] = pd.cut(
            df['view_count'],
            bins=[0, 10000, 100000, 1000000, 10000000, np.inf],
            labels=['Micro', 'Small', 'Medium', 'Large', 'Enterprise']
        )
        
        # Convert to string for mapping
        df['channel_tier'] = df['channel_tier'].astype(str)
        
        # Size-based adjustment (larger channels get better terms)
        tier_multipliers = {'Micro': 1.2, 'Small': 1.1, 'Medium': 1.0, 'Large': 0.9, 'Enterprise': 0.8}
        df['tier_multiplier'] = df['channel_tier'].map(tier_multipliers).fillna(1.0)
        
        # Calculate final metrics
        df['ad_density'] = df['base_ad_rate'] * df['tier_multiplier']
        df['estimated_ads'] = (df['ad_density'] * df['duration_minutes']).clip(lower=1).round()
        df['ad_time_seconds'] = df['estimated_ads'] * 20
        df['ad_ratio'] = (df['ad_time_seconds'] / df['duration_seconds'] * 100).round(2)
        
        df['engagement_rate'] = (
            (df['like_count'] + df['comment_count']) / df['view_count'].replace(0, 1) * 100
        ).round(3)
        
        df['revenue_pressure'] = (
            df['ad_density'] * 0.5 +
            df['ad_ratio'] * 0.01 * 0.3 +
            (df['view_count'] / 1e6) * 0.2
        ).round(3)
        
        self.processed = df
        return df
    
    def add_features(self):
        """Add derived features for analysis."""
        df = self.processed.copy()
        df['age_days'] = (pd.Timestamp.now().tz_localize(None) - df['published_at'].dt.tz_localize(None)).dt.days
        df['duration_category'] = pd.cut(
            df['duration_minutes'],
            bins=[0, 5, 15, 30, np.inf],
            labels=['Short', 'Medium', 'Long', 'Very Long']
        )
        df['views_per_day'] = (df['view_count'] / df['age_days'].replace(0, 1)).round()
        self.processed = df
        return df
    
    def process_pipeline(self):
        """Run full transformation pipeline."""
        self.clean_data()
        self.calculate_metrics()
        self.add_features()
        print(f"Processed {len(self.processed)} videos")
        return self.processed
    
    def save_processed(self, filename: str = None):
        """Save processed data."""
        if self.processed is None:
            raise ValueError("No processed data to save")
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'processed_{timestamp}.csv'
        filepath = os.path.join('data', 'processed', filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.processed.to_csv(filepath, index=False)
        print(f"Saved to {filepath}")
        return filepath


if __name__ == '__main__':
    import glob
    files = glob.glob('data/raw/youtube_data_*.csv')
    if not files:
        print("No raw data found. Run extract.py first.")
        exit()
    latest = max(files, key=os.path.getctime)
    transformer = DataTransformer(latest)
    transformer.load_data()
    df = transformer.process_pipeline()
    transformer.save_processed()
    print("\nSummary by category:")
    summary = df.groupby('category_name').agg({
        'ad_density': 'mean',
        'ad_ratio': 'mean',
        'video_id': 'count'
    }).round(3)
    print(summary)
