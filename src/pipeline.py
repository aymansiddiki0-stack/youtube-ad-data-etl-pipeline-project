"""
Main ETL pipeline orchestrator.
"""

import sys
from .extract import YouTubeExtractor
from .transform import DataTransformer


def run_pipeline(categories=None, videos_per_category=30):
    """Execute the full ETL pipeline."""
    
    # Extract
    extractor = YouTubeExtractor()
    df_raw = extractor.collect_by_categories(
        category_ids=categories,
        videos_per_category=videos_per_category
    )
    
    if len(df_raw) == 0:
        print("No data collected. Check API key and quota.")
        sys.exit(1)
    
    raw_file = extractor.save_data(df_raw)
    
    # Transform
    transformer = DataTransformer(raw_file)
    transformer.load_data()
    df_processed = transformer.process_pipeline()
    transformer.save_processed()
    
    # Summary
    
    print("Transformation Summary")
    print(f"Videos collected: {len(df_processed)}")
    print(f"Categories: {df_processed['category_name'].nunique()}")
    print(f"Avg ad density: {df_processed['ad_density'].mean():.3f} ads/min")
    print(f"Avg ad ratio: {df_processed['ad_ratio'].mean():.2f}%")
    
    return df_processed


if __name__ == '__main__':
    # Run with default settings
    run_pipeline(videos_per_category=30)
