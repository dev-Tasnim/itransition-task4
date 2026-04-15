import streamlit as st
import pandas as pd
import os
from task4 import results  

st.set_page_config(layout="wide", page_title="Bookstore BI Dashboard")

st.title("Bookstore Analytics Dashboard")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["DATA 1", "DATA 2", "DATA 3"])

def display_dashboard(data_key, tab_object):
    with tab_object:
        if data_key not in results:
            st.error(f"No results found for {data_key}")
            return
            
        res = results[data_key]
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Unique Users", res['users'])
        col2.metric("Unique Author Sets", res['authors'])
        col3.subheader("Most Popular Author")
        col3.write(f"**{res['popular_author']}**")

        st.markdown("---")

        left_col, right_col = st.columns([2, 1])
        
        with left_col:
            st.subheader("Daily Revenue Trend")
            img_path = f"daily_revenue_{data_key}.png"
            if os.path.exists(img_path):
                st.image(img_path)
            else:
                st.warning(f"Image '{img_path}' not found. Run task4.py first.")

        with right_col:
            st.subheader("Top 5 Revenue Days")
            top_5_df = res['top_5'].reset_index()
            top_5_df.columns = ['Date', 'Revenue ($)']
            st.table(top_5_df)

        st.markdown("---")
        st.subheader("Top Customer (Best Buyer)")
        st.write(f"Associated IDs: {res['best_buyer']}")

display_dashboard("DATA1", tab1)
display_dashboard("DATA2", tab2)
display_dashboard("DATA3", tab3)