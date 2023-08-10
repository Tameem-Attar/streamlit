import streamlit as st
import pandas as pd
from datetime import datetime
from data_storage import DataStorage

def show_recommendation_section():
    st.title("Recommendation Section")
    
    # Create a select box to choose the type of recommendations
    recommendation_type = st.selectbox("Select Recommendation Type:", ["Warehouse Recommendations", "Query History Recommendations"])

    if recommendation_type == "Warehouse Recommendations":
        show_warehouse_recommendations()
    elif recommendation_type == "Query History Recommendations":
        show_query_history_recommendations()

def show_warehouse_recommendations():
    st.subheader("Warehouse Recommendations")
    
    # Sample data for demonstration
    data_storage = DataStorage()
    data_storage.connect_to_postgres()
    data = data_storage.fetch_data_from_postgres()
    warehouse_data = pd.DataFrame(data, columns=["START_TIME", "END_TIME", "WAREHOUSE_ID", "WAREHOUSE_NAME", "CREDITS_USED", "CREDITS_USED_COMPUTE", "CREDITS_USED_CLOUD_SERVICES"])

    # Calculate peak usage for each warehouse
    peak_usage_by_warehouse = warehouse_data.groupby("WAREHOUSE_NAME")["CREDITS_USED"].max()

    # Identify warehouses with high peak usage
    high_peak_warehouses = peak_usage_by_warehouse[peak_usage_by_warehouse > 1.0]

    # Display recommendations for high peak usage warehouses
    st.subheader("Warehouses with High Peak Usage:")
    for warehouse_name, peak_credits_used in high_peak_warehouses.items():
        st.write(f"- Consider scaling up the warehouse size for {warehouse_name} during peak times.")
        st.write("- Optimize queries to reduce resource consumption.")
        st.write("\n")

    # Calculate the ratio of compute usage to cloud services usage
    warehouse_data["COMPUTE_TO_CLOUD_RATIO"] = (
        warehouse_data["CREDITS_USED_COMPUTE"] / warehouse_data["CREDITS_USED_CLOUD_SERVICES"]
    )

    # Identify warehouses with high compute to cloud services ratio
    high_ratio_warehouses = warehouse_data[warehouse_data["COMPUTE_TO_CLOUD_RATIO"] > 5.0]

    # Display recommendations for high compute-to-cloud services ratio warehouses
    st.subheader("Warehouses with High Compute-to-Cloud Services Ratio:")
    for warehouse_name in high_ratio_warehouses["WAREHOUSE_NAME"].unique():
        st.write(f"- Optimize compute-intensive queries for {warehouse_name} to reduce compute resource consumption.")
        st.write("- Monitor cloud services usage to ensure cost efficiency.")
        st.write("\n")

def show_query_history_recommendations():
    st.subheader("Query History Recommendations")

    # Load query history data from CSV (replace with actual data source)
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
    df["EXECUTION_TIME_SEC"] = (df["END_TIME"] - df["START_TIME"]).dt.total_seconds()


    # Title for the recommendations section
    st.title("Query Recommendations")

    # Calculate average execution time for queries
    df["EXECUTION_TIME_SEC"] = pd.to_numeric(df["EXECUTION_TIME"], errors="coerce")
    avg_execution_time = df["EXECUTION_TIME_SEC"].mean()

    # Calculate average bytes scanned for queries
    df["BYTES_SCANNED"] = pd.to_numeric(df["BYTES_SCANNED"], errors="coerce")
    avg_bytes_scanned = df["BYTES_SCANNED"].mean()

    # Display high-level insights and recommendations
    st.subheader("Overall Insights:")
    st.write(f"Average Execution Time: {avg_execution_time:.2f} seconds")
    st.write(f"Average Bytes Scanned: {avg_bytes_scanned:.2f} bytes")
    st.write("Consider optimizing queries and reviewing data usage patterns for better performance.")


