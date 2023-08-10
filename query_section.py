import streamlit as st
from datetime import datetime, timezone, timedelta
import pandas as pd
from data_storage import DataStorage  # Import your DataStorage class

def show_query_history_section():
    st.title("Query History Insights")

    # Create an instance of the DataStorage class
    data_storage = DataStorage()

    # Connect to the database (you can choose PostgreSQL or Snowflake)
    data_storage.connect_to_postgres()  # You can switch to Snowflake if needed

    # Fetch data from the query_history table using your DataStorage class
    data = data_storage.fetch_query_history_from_postgres()  # Update with your actual method

    # Define column names based on your table schema
    columns = [
    "QUERY_ID", "QUERY_TEXT", "DATABASE_ID", "DATABASE_NAME", "SCHEMA_ID", "SCHEMA_NAME", "QUERY_TYPE", "SESSION_ID",
    "USER_NAME", "ROLE_NAME", "WAREHOUSE_ID", "WAREHOUSE_NAME", "WAREHOUSE_SIZE", "WAREHOUSE_TYPE", "CLUSTER_NUMBER",
    "QUERY_TAG", "EXECUTION_STATUS", "ERROR_CODE", "ERROR_MESSAGE", "START_TIME", "END_TIME", "TOTAL_ELAPSED_TIME",
    "BYTES_SCANNED", "PERCENTAGE_SCANNED_FROM_CACHE", "BYTES_WRITTEN", "BYTES_WRITTEN_TO_RESULT",
    "BYTES_READ_FROM_RESULT", "ROWS_PRODUCED", "ROWS_INSERTED", "ROWS_UPDATED", "ROWS_DELETED", "ROWS_UNLOADED",
    "BYTES_DELETED", "PARTITIONS_SCANNED", "PARTITIONS_TOTAL", "BYTES_SPILLED_TO_LOCAL_STORAGE",
    "BYTES_SPILLED_TO_REMOTE_STORAGE", "BYTES_SENT_OVER_THE_NETWORK", "COMPILATION_TIME", "EXECUTION_TIME",
    "QUEUED_PROVISIONING_TIME", "QUEUED_REPAIR_TIME", "QUEUED_OVERLOAD_TIME", "TRANSACTION_BLOCKED_TIME",
    "OUTBOUND_DATA_TRANSFER_CLOUD", "OUTBOUND_DATA_TRANSFER_REGION", "OUTBOUND_DATA_TRANSFER_BYTES",
    "INBOUND_DATA_TRANSFER_CLOUD", "INBOUND_DATA_TRANSFER_REGION", "INBOUND_DATA_TRANSFER_BYTES",
    "LIST_EXTERNAL_FILES_TIME", "CREDITS_USED_CLOUD_SERVICES", "RELEASE_VERSION",
    "EXTERNAL_FUNCTION_TOTAL_INVOCATIONS", "EXTERNAL_FUNCTION_TOTAL_SENT_ROWS", "EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS",
    "EXTERNAL_FUNCTION_TOTAL_SENT_BYTES", "EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES", "QUERY_LOAD_PERCENT",
    "IS_CLIENT_GENERATED_STATEMENT", "QUERY_ACCELERATION_BYTES_SCANNED", "QUERY_ACCELERATION_PARTITIONS_SCANNED",
    "QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR", "TRANSACTION_ID", "CHILD_QUERIES_WAIT_TIME", "ROLE_TYPE"]  # Add all column names

    # Create a DataFrame from the fetched data
    df = pd.DataFrame(data, columns=columns)
    

    # Convert timestamps to datetime objects
    df["START_TIME"] = pd.to_datetime(df["START_TIME"], utc=True)
    df["END_TIME"] = pd.to_datetime(df["END_TIME"], utc=True)

    # Calculate query execution time in seconds
    df["EXECUTION_TIME_SEC"] = (df["END_TIME"] - df["START_TIME"]).dt.total_seconds()

    # Predefined date range options
    st.header("Predefined Date Ranges")
    selected_option = st.selectbox("Select a predefined date range:", ["Whole Dataset", "Past 1 Month", "Past 2 Months", "Past 3 Months", "Past 6 Months", "Custom Date Range"])

    if selected_option == "Whole Dataset":
        filtered_data = df
    elif selected_option == "Past 1 Month":
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=30)
        filtered_data = df[(df["START_TIME"] >= start_date) & (df["START_TIME"] <= end_date)]
    elif selected_option == "Past 2 Months":
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=60)
        filtered_data = df[(df["START_TIME"] >= start_date) & (df["START_TIME"] <= end_date)]
    # ... (repeat for other predefined options)
    elif selected_option == "Custom Date Range":
        start_date = st.date_input("Start Date")
        end_date = st.date_input("End Date")
        if start_date is not None and end_date is not None:
            # Calculate the timestamp range for the selected date range
            start_of_day = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
            end_of_day = datetime.combine(end_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1) - timedelta(microseconds=1)
            
            # Filter data for the selected timestamp range
            filtered_data = df[(df["START_TIME"] >= start_of_day) & (df["START_TIME"] <= end_of_day)]
        else:
            st.warning("Please select valid start and end dates.")
    else:
        st.error("Invalid option selected")

    # Calculate insights based on the filtered data
    total_queries = len(filtered_data)
    total_days = (filtered_data["START_TIME"].max() - filtered_data["START_TIME"].min()).days + 1
    avg_queries_per_day = total_queries / total_days
    #failed_queries = df[df["EXECUTION_STATUS"] == "FAILED"]["EXECUTION_STATUS"].count()
    avg_execution_time = df["EXECUTION_TIME_SEC"].mean()

    # Display insights
    st.subheader("Query Insights")
    st.text(f"Total Queries: {total_queries}")
    st.text(f"Total Number of Days: {total_days}")
    st.text(f"Average Number of Queries per Day: {avg_queries_per_day:.2f}")
    st.text(f"Avg Execution Time (seconds): {avg_execution_time:.2f}")
    #st.text(f"Failed Queries: {failed_queries}")

    
    # Create a DataFrame from the fetched data
    df = pd.DataFrame(data, columns=columns)
    # Group data by username and warehouse name
    grouped_data = filtered_data.groupby(["USER_NAME", "WAREHOUSE_NAME"]).agg({
        "QUERY_ID": "count",
        "EXECUTION_TIME_SEC": "sum",
        "CREDITS_USED_CLOUD_SERVICES": "sum"
    }).reset_index()

    # Display select boxes for filtering
    st.subheader("Filter by User and Warehouse")
    
    # Option to select all users
    users = grouped_data["USER_NAME"].unique()
    all_users_option = "All Users"
    selected_username = st.selectbox("Select a username:", [all_users_option] + list(users))
    
    # Option to select all warehouses
    warehouses = grouped_data["WAREHOUSE_NAME"].unique()
    all_warehouses_option = "Select All"
    selected_warehouse = st.selectbox("Select a warehouse:", [all_warehouses_option] + list(warehouses))

    # Filter data by selected username and warehouse
    if selected_username == all_users_option:
        filtered_grouped_data = grouped_data
    else:
        filtered_grouped_data = grouped_data[(grouped_data["USER_NAME"] == selected_username)]

    if selected_warehouse != all_warehouses_option:
        filtered_grouped_data = filtered_grouped_data[filtered_grouped_data["WAREHOUSE_NAME"] == selected_warehouse]

    # Display individual bar graphs for Number of Queries, Execution Time, and Cost
    st.subheader("Number of Queries by User and Warehouse")
    st.bar_chart(filtered_grouped_data[["USER_NAME", "WAREHOUSE_NAME", "QUERY_ID"]], 
                 x="USER_NAME", y="QUERY_ID", use_container_width=True)

    st.subheader("Execution Time by User and Warehouse")
    st.bar_chart(filtered_grouped_data[["USER_NAME", "WAREHOUSE_NAME", "EXECUTION_TIME_SEC"]], 
                 x="USER_NAME", y="EXECUTION_TIME_SEC", use_container_width=True)

    st.subheader("Cost by User and Warehouse")
    st.bar_chart(filtered_grouped_data[["USER_NAME", "WAREHOUSE_NAME", "CREDITS_USED_CLOUD_SERVICES"]], 
                 x="USER_NAME", y="CREDITS_USED_CLOUD_SERVICES", use_container_width=True)
 


#####################################################
# ... (existing code)

   # Group data by username and warehouse name
    grouped_data = df.groupby(["USER_NAME", "WAREHOUSE_NAME"]).agg({
        "QUERY_ID": "count",
        "EXECUTION_TIME": "sum",
        "CREDITS_USED_CLOUD_SERVICES": "sum",
        "START_TIME": "first"  # Keep the first START_TIME for past months insights
    }).reset_index()

    # Display select boxes for filtering
    st.subheader("Filter by User and Warehouse")

    # Option to select all users
    users = grouped_data["USER_NAME"].unique()
    all_users_option = "All Users"
    selected_username = st.selectbox("Select a username:", [all_users_option] + list(users), key="user_select")

    # Option to select all warehouses
    warehouses = grouped_data["WAREHOUSE_NAME"].unique()
    all_warehouses_option = "Select All"
    selected_warehouse = st.selectbox("Select a warehouse:", [all_warehouses_option] + list(warehouses), key="warehouse_select")

    # Filter data by selected username and warehouse
    if selected_username == all_users_option:
        filtered_grouped_data = grouped_data
    else:
        filtered_grouped_data = grouped_data[(grouped_data["USER_NAME"] == selected_username)]

    if selected_warehouse != all_warehouses_option:
        filtered_grouped_data = filtered_grouped_data[filtered_grouped_data["WAREHOUSE_NAME"] == selected_warehouse]

    # Display bar graphs for Number of queries, Execution time, and Cost
    st.subheader("Past Months Insights")

    # Group by month and calculate aggregates for past months insights
    past_months_data = filtered_grouped_data.groupby(filtered_grouped_data["START_TIME"].dt.to_period("M")).agg({
        "QUERY_ID": "sum",
        "EXECUTION_TIME": "sum",
        "CREDITS_USED_CLOUD_SERVICES": "sum"
    }).reset_index()

    # Convert the "START_TIME" column to datetime and create a new column "Month"
    past_months_data["Month"] = past_months_data["START_TIME"].dt.strftime("%b %Y")

    # Display bar chart for Number of queries
    st.subheader("Number of Queries - Past Months")
    st.bar_chart(past_months_data[["Month", "QUERY_ID"]].set_index("Month"), use_container_width=True)

    # Display bar chart for Execution time
    st.subheader("Execution Time - Past Months")
    st.bar_chart(past_months_data[["Month", "EXECUTION_TIME"]].set_index("Month"), use_container_width=True)

    # Display bar chart for Cost
    st.subheader("Cost - Past Months")
    st.bar_chart(past_months_data[["Month", "CREDITS_USED_CLOUD_SERVICES"]].set_index("Month"), use_container_width=True)

    # ... (rest of your code)


    

    
    


    

    



