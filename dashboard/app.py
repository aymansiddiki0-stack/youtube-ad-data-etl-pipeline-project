"""
StreamDims Dashboard - YouTube Ad Analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import glob
import os

st.set_page_config(page_title="YouTube Ad Saturation Analytics", layout="wide")


@st.cache_data
def load_data():
    """Load most recent processed data."""
    files = glob.glob('../data/processed/processed_*.csv')
    if not files:
        return None
    
    latest = max(files, key=os.path.getctime)
    df = pd.read_csv(latest)
    df['published_at'] = pd.to_datetime(df['published_at'])
    return df


def main():
    st.title("YouTube Ad Saturation Analytics")
    
    df = load_data()
    
    if df is None:
        st.error("No data found. Run the ETL pipeline first.")
        st.code("python src/pipeline.py")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    selected_categories = st.sidebar.multiselect(
        "Categories",
        options=sorted(df['category_name'].unique()),
        default=df['category_name'].unique()
    )
    
    min_views = st.sidebar.number_input(
        "Minimum Views",
        min_value=0,
        value=0,
        step=1000
    )
    
    # Apply filters
    filtered_df = df[
        (df['category_name'].isin(selected_categories)) &
        (df['view_count'] >= min_views)
    ]
    
    st.sidebar.markdown(f"**{len(filtered_df)} videos selected**")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Videos", f"{len(filtered_df):,}")
    
    with col2:
        st.metric("Avg Ad Density", f"{filtered_df['ad_density'].mean():.3f}")
    
    with col3:
        st.metric("Avg Ad Ratio", f"{filtered_df['ad_ratio'].mean():.1f}%")
    
    with col4:
        st.metric("Categories", filtered_df['category_name'].nunique())
    
    st.markdown("---")
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "Category Analysis",
        "Channel Insights", 
        "Top Videos",
        "Data Explorer"
    ])
    
    with tab1:
        st.subheader("Ad Metrics by Category")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Bar chart
            cat_stats = filtered_df.groupby('category_name').agg({
                'ad_density': 'mean',
                'ad_ratio': 'mean',
                'video_id': 'count'
            }).reset_index()
            cat_stats.columns = ['Category', 'Ad Density', 'Ad Ratio', 'Count']
            cat_stats = cat_stats.sort_values('Ad Density', ascending=False)
            
            fig = px.bar(
                cat_stats,
                x='Category',
                y='Ad Density',
                color='Ad Ratio',
                title='Average Ad Density by Category',
                labels={'Ad Density': 'Ads per Minute'},
                color_continuous_scale='Reds'
            )
            fig.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Revenue pressure box plot
            fig2 = px.box(
                filtered_df,
                x='category_name',
                y='revenue_pressure',
                title='Revenue Pressure Distribution',
                labels={'category_name': 'Category', 'revenue_pressure': 'Revenue Pressure'}
            )
            fig2.update_layout(xaxis_tickangle=-45, showlegend=False, height=400)
            st.plotly_chart(fig2, use_container_width=True)
        
        # Heatmap
        st.markdown("#### Ad Ratio by Category and Channel Size")
        pivot_data = filtered_df.pivot_table(
            values='ad_ratio',
            index='category_name',
            columns='channel_tier',
            aggfunc='mean'
        )
        
        # Reorder columns from smallest to largest channel size
        tier_order = ['Micro', 'Small', 'Medium', 'Large', 'Enterprise']
        pivot_data = pivot_data[[col for col in tier_order if col in pivot_data.columns]]
        
        fig_heat = px.imshow(
            pivot_data,
            labels=dict(x="Channel Tier", y="Category", color="Ad Ratio (%)"),
            color_continuous_scale='RdYlGn_r',
            aspect='auto'
        )
        fig_heat.update_layout(height=450)
        st.plotly_chart(fig_heat, use_container_width=True)
    
    with tab2:
        st.subheader("Channel Size Analysis")
        
        # Scatter plot
        fig = px.scatter(
            filtered_df.sample(min(500, len(filtered_df))),
            x='view_count',
            y='ad_density',
            color='category_name',
            size='duration_minutes',
            hover_data=['channel_title', 'title', 'ad_ratio'],
            title='View Count vs Ad Density',
            labels={
                'view_count': 'Views',
                'ad_density': 'Ad Density',
                'category_name': 'Category'
            },
            log_x=True
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        # Tier breakdown
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### By Channel Tier")
            tier_stats = filtered_df.groupby('channel_tier').agg({
                'ad_density': 'mean',
                'ad_ratio': 'mean',
                'video_id': 'count'
            }).round(3)
            tier_stats.columns = ['Avg Ad Density', 'Avg Ad Ratio', 'Count']
            
            # Reorder from smallest to largest
            tier_order = ['Micro', 'Small', 'Medium', 'Large', 'Enterprise']
            tier_stats = tier_stats.reindex([t for t in tier_order if t in tier_stats.index])
            tier_stats.index.name = 'Channel Tier'
            
            st.dataframe(tier_stats, use_container_width=True)
        
        with col2:
            # Duration vs ads
            fig_dur = px.scatter(
                filtered_df,
                x='duration_minutes',
                y='estimated_ads',
                color='category_name',
                title='Duration vs Estimated Ads',
                labels={
                    'duration_minutes': 'Duration (min)',
                    'estimated_ads': 'Estimated Ads',
                    'category_name': 'Category'
                }
            )
            fig_dur.update_layout(height=350)
            st.plotly_chart(fig_dur, use_container_width=True)
    
    with tab3:
        st.subheader("Most & Least Monetized Videos")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Highest Revenue Pressure")
            top_10 = filtered_df.nlargest(10, 'revenue_pressure')[
                ['title', 'channel_title', 'category_name', 'ad_ratio', 'revenue_pressure']
            ]
            st.dataframe(top_10, use_container_width=True, hide_index=True)
        
        with col2:
            st.markdown("#### Lowest Revenue Pressure")
            bottom_10 = filtered_df.nsmallest(10, 'revenue_pressure')[
                ['title', 'channel_title', 'category_name', 'ad_ratio', 'revenue_pressure']
            ]
            st.dataframe(bottom_10, use_container_width=True, hide_index=True)
        
        # Most viewed
        st.markdown("---")
        st.markdown("#### Most Viewed Videos")
        top_viewed = filtered_df.nlargest(10, 'view_count')[
            ['title', 'channel_title', 'view_count', 'ad_density', 'ad_ratio']
        ]
        st.dataframe(top_viewed, use_container_width=True, hide_index=True)
    
    with tab4:
        st.subheader("Data Explorer")
        
        # Search
        search = st.text_input("Search videos or channels")
        
        if search:
            search_df = filtered_df[
                filtered_df['title'].str.contains(search, case=False, na=False) |
                filtered_df['channel_title'].str.contains(search, case=False, na=False)
            ]
        else:
            search_df = filtered_df
        
        # Sort options
        col1, col2 = st.columns([3, 1])
        with col1:
            sort_by = st.selectbox(
                "Sort by",
                ['ad_density', 'ad_ratio', 'view_count', 'revenue_pressure', 'duration_minutes']
            )
        with col2:
            sort_order = st.selectbox("Order", ['Descending', 'Ascending'])
        
        # Display
        display_cols = [
            'title', 'channel_title', 'category_name', 'channel_tier',
            'duration_minutes', 'view_count', 'ad_density', 'ad_ratio', 'revenue_pressure'
        ]
        
        display_df = search_df[display_cols].sort_values(
            sort_by, 
            ascending=(sort_order == 'Ascending')
        ).head(100)
        
        st.write(f"Showing {len(display_df)} videos")
        st.dataframe(display_df, use_container_width=True, height=500)
        
        # Download button
        csv = display_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download as CSV",
            csv,
            "streamdims_export.csv",
            "text/csv"
        )


if __name__ == '__main__':
    main()