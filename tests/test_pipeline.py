"""
Unit tests for pipeline.
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from transform import DataTransformer


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({
        'video_id': ['vid1', 'vid2', 'vid3'],
        'title': ['Video 1', 'Video 2', 'Video 3'],
        'channel_id': ['ch1', 'ch2', 'ch3'],
        'channel_title': ['Channel 1', 'Channel 2', 'Channel 3'],
        'category_id': ['10', '20', '24'],
        'category_name': ['Music', 'Gaming', 'Entertainment'],
        'published_at': ['2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z', '2024-01-03T00:00:00Z'],
        'duration_seconds': [180, 600, 300],
        'view_count': [1000, 5000, 2000],
        'like_count': [100, 500, 200],
        'comment_count': [10, 50, 20],
        'collected_at': ['2024-01-01T00:00:00Z'] * 3
    })


def test_transformer_init():
    """Test transformer initialization."""
    transformer = DataTransformer()
    assert transformer.data is None
    assert transformer.processed is None


def test_clean_data(sample_data):
    """Test data cleaning."""
    transformer = DataTransformer()
    transformer.data = sample_data
    
    cleaned = transformer.clean_data()
    
    assert len(cleaned) == 3
    assert not cleaned.isnull().any().any()
    assert pd.api.types.is_datetime64_any_dtype(cleaned['published_at'])


def test_calculate_metrics(sample_data):
    """Test metric calculations."""
    transformer = DataTransformer()
    transformer.data = sample_data
    transformer.clean_data()
    
    result = transformer.calculate_metrics()
    
    # Check required columns exist
    required_cols = ['ad_density', 'estimated_ads', 'ad_ratio', 'revenue_pressure']
    for col in required_cols:
        assert col in result.columns
    
    # Check value ranges
    assert (result['ad_density'] > 0).all()
    assert (result['estimated_ads'] >= 1).all()
    assert (result['ad_ratio'] >= 0).all()
    assert (result['ad_ratio'] <= 100).all()


def test_pipeline_execution(sample_data, tmp_path):
    """Test full pipeline."""
    # Save sample data
    input_file = tmp_path / "test_input.csv"
    sample_data.to_csv(input_file, index=False)
    
    transformer = DataTransformer(str(input_file))
    transformer.load_data()
    result = transformer.process_pipeline()
    
    assert len(result) == 3
    assert 'ad_density' in result.columns
    assert 'revenue_pressure' in result.columns
