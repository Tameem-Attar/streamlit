import streamlit as st
from datetime import datetime, timezone, timedelta
import hydralit_components as hc
import pandas as pd
from data_storage import DataStorage
from warehouse_section import show_warehouse_section
from query_section import show_query_history_section
from recommedation_section import show_recommendation_section
#from test3 import show_query_history_section1
import snowflake.connector
import psycopg2

st.set_page_config(layout="wide")
# Menu data for navigation bar
menu_data = [
    {'icon': "fas fa-warehouse", 'label': "Warehouse"},
    {'icon': "fas fa-database", 'label': "Query"},
    {'icon': "fas fa-database", 'label': "Storage"},
    {'icon': "fas fa-balance-scale", 'label': "Governance"},
    {'icon': "fas fa-dollar-sign", 'label': "Cost"},
    {'icon': "fas fa-lightbulb", 'label': "Recommendations"},
]

# Override theme for inactive text color
over_theme = {'txc_inactive': '#FFFFFF'}

# Create the navigation bar
menu_id = hc.nav_bar(menu_definition=menu_data, home_name='Home', override_theme=over_theme)

# Set page configuration to wide layout
#st.set_page_config(layout="wide")

if menu_id == 'Home':
    background_image = "Welcome to Snowflake Cost Optimizer.png"
    st.image(background_image, use_column_width=True)
elif menu_id == 'Warehouse':  
    show_warehouse_section()
elif menu_id == 'Query':
    show_query_history_section()
elif menu_id == 'Recommendations':
    show_recommendation_section()
    

    
    


