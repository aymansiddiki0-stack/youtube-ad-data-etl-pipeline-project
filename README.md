# YouTube live Ad Saturation ETL & Dashboard 

This is a project that builds a live ETL pipeline and interactive dashboard for analyzing YouTube ad saturation.
The pipeline connects directly to the YouTube Data API v3, fetching live video and channel metadata, computing ad-related metrics, and visualizing results through a Streamlit dashboard.

### Tech Stack
- Python 3.11
- API Key: YouTube Data API v3
- Libraries: pandas, numpy, isodate
- Visualization: Streamlit, Plotly
- Environment: Docker (optional), Windows Command Prompt
- Utilities: dotenv, requests, logging, pytest

### Key Features
- Pulls real-time metadata from YouTube using API keys.
- ETL Pipeline: Cleans, transforms, and enriches raw video and channel data.
- Metrics: Calculates ad_density, ad_ratio, duration_minutes, and other engagement metrics.
- Dashboard: Interactive Streamlit interface with category filters, KPIs, and visual analytics.
- Fully configurable via .env and requirements.txt.

#### Setup 
1. Clone the repository
2. Set your API key:
    - Create a file named .env in the project root: YOUTUBE_API_KEY=your_api_key_here (copy paste this and replace with your api key.)
3. Create and activate a virtual environment:
    - python -m venv .venv
    - .venv\Scripts\activate
4. Install dependencies:
    - python -m pip install --upgrade pip
    - pip install -r requirements.txt
5. Run the ETL pipeline:
    - python src\pipeline.py

### This script:
- Connects to the YouTube API
- Downloads raw video/channel data to data\raw\
- Cleans and transforms it into data\processed\ with calculated metrics
#### Running the Dashboard
- Once data is processed, launch the Streamlit dashboard:
    - cd dashboard
    - streamlit run app.py
- Then open your browser to http://localhost:8501 .
#### The dashboard provides:
- Category and view filters
- Average ad density and ratio summaries
- Top/Bottom video tables
- Interactive charts built with Plotly

#### Configuration
All runtime settings are defined in:
- .env – API key and secrets
- docker-compose.yml (optional container setup)
- src\pipeline.py – categories, limits, and feature calculations

#### Default output folders:
- data\raw\
- data\processed\

#### Testing
Run automated tests to validate the transform logic:
- pytest -v

### License
MIT License