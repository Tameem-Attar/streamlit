import streamlit as st
from datetime import datetime, timezone, timedelta
from data_storage import DataStorage

def show_warehouse_section():
    st.title("Warehouse Insights")

    data_storage = DataStorage()
        # Connect to the chosen database
    st.title("Choose where to fetch data:")
    choice = st.radio("Select Database:", ["PostgreSQL", "Snowflake"])

    if choice == "PostgreSQL":
        data_storage.connect_to_postgres()
        data = data_storage.fetch_data_from_postgres()
    elif choice == "Snowflake":
        data_storage.connect_to_snowflake()
        data = data_storage.fetch_data_from_snowflake()

    # Predefined date range options
    st.header("Predefined Date Ranges")
    option = st.selectbox("Select a predefined date range:", ["Whole Dataset", "Past 1 Month", "Past 2 Months", "Past 3 Months", "Past 6 Months", "Custom Date Range"])

    if option == "Whole Dataset":
        filtered_data = data
    else:
        if option == "Past 1 Month":
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
        elif option == "Past 2 Months":
            end_date = datetime.now()
            start_date = end_date - timedelta(days=60)
        elif option == "Past 3 Months":
            end_date = datetime.now()
            start_date = end_date - timedelta(days=90)
        elif option == "Past 6 Months":
            end_date = datetime.now()
            start_date = end_date - timedelta(days=180)
        elif option == "Custom Date Range":
            start_date = st.date_input("Start Date")
            end_date = st.date_input("End Date")
            if start_date is not None and end_date is not None:
                # Calculate the timestamp range for the selected date range
                start_of_day = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
                end_of_day = datetime.combine(end_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1) - timedelta(microseconds=1)
                
                # Filter data for the selected timestamp range
                filtered_data = [row for row in data if start_of_day <= row[0] <= end_of_day]
            else:
                st.warning("Please select valid start and end dates.")

    # ...

    if option == "Whole Dataset":
        st.text("Selected Date Range: Whole Dataset")
    elif option == "Custom Date Range":
        start_date_text = start_date.strftime("%Y-%m-%d")
        end_date_text = end_date.strftime("%Y-%m-%d")
        st.text(f"Selected Date Range: {start_date_text} to {end_date_text}")
    else:
        st.text(f"Selected Date Range: {option}")

        # Calculate the timestamp range for the selected date range
        start_of_day = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, 0, tzinfo=timezone.utc)
        end_of_day = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, 999999, tzinfo=timezone.utc)

        # Filter data for the selected timestamp range
        filtered_data = [row for row in data if start_of_day <= row[0] <= end_of_day]

    # Calculate total number of warehouses
    total_warehouses = len(set(row[3] for row in filtered_data))

    # Calculate total credits consumed
    total_credits_consumed = sum(row[4] for row in filtered_data)

    # Calculate total compute cost
    price_per_credit = 0.2
    total_compute_cost = sum(row[5] * price_per_credit for row in filtered_data)

    # Convert data into DataFrame for further analysis
    import pandas as pd
    df = pd.DataFrame(filtered_data, columns=["START_TIME", "END_TIME", "WAREHOUSE_ID", "WAREHOUSE_NAME", "CREDITS_USED", "CREDITS_USED_COMPUTE", "CREDITS_USED_CLOUD_SERVICES"])

    # Convert the "START_TIME" column to UTC timezone
    df["START_TIME"] = pd.to_datetime(df["START_TIME"], utc=True)
    df["END_TIME"] = pd.to_datetime(df["END_TIME"], utc=True)

    # Extract the date for grouping
    df["day"] = df["START_TIME"].dt.date

    # Calculate average daily credits used and average daily cost
    daily_credits_used = df.groupby("day")["CREDITS_USED"].sum()
    average_daily_credits_used = daily_credits_used.mean()
    average_daily_cost = average_daily_credits_used * price_per_credit

    # Group the DataFrame by warehouse
    grouped_df = df.groupby("WAREHOUSE_NAME")["CREDITS_USED"].sum()

    # Calculate the cost of credits used in dollars by warehouse
    cost_of_credits_used_by_warehouse = grouped_df * price_per_credit

    # Create the cost_df DataFrame
    cost_df = pd.DataFrame({
        "Cost of Credits Used by Warehouse": cost_of_credits_used_by_warehouse,
    })

    # Display the results in Streamlit
    st.title("Warehouse Insights")

    if option == "Whole Dataset":
        st.text("Selected Date Range: Whole Dataset")
    else:
        st.text(f"Selected Date Range: {start_date.date()} to {end_date.date()}")

    st.text(f"Total Number of Warehouses: {total_warehouses}")
    st.text(f"Total Credits Consumed: {total_credits_consumed:.2f}")
    st.text(f"Total Compute Cost: ${total_compute_cost:.2f}")
    st.text(f"Average Daily Credits Used: {average_daily_credits_used:.2f}")
    st.text(f"Average Daily Cost: ${average_daily_cost:.2f}")


    # Display the bar chart
    st.title("Credits Used by Warehouse")
    st.bar_chart(grouped_df)

    # Display the results in two columns layout
    col1, col2 = st.columns(2)

    # Display the table in the first column
    col1.title("Cost of Credits Used by Warehouse")
    col1.table(cost_df)

    # Display the bar
    col2.title("Cost of Credits Used by Warehouse (Graph)")
    col2.bar_chart(cost_df)

    # Display the table of top warehouses by usage in the first column
    num_top_warehouses = 5  # You can adjust this number as needed
    top_warehouses = cost_df.sort_values(by="Cost of Credits Used by Warehouse", ascending=False).head(num_top_warehouses)

    # Calculate the percentage of credits used and respective cost for the top warehouses
    top_warehouses["Percentage of Credits Used"] = (top_warehouses["Cost of Credits Used by Warehouse"] / total_compute_cost) * 100
    top_warehouses["Respective Cost"] = top_warehouses["Cost of Credits Used by Warehouse"]

    # Display the results in two columns layout
    col1, col2 = st.columns(2)

    # Display the table of top warehouses by usage in the first column
    num_top_warehouses = 5  # You can adjust this number as needed
    top_warehouses = cost_df.sort_values(by="Cost of Credits Used by Warehouse", ascending=False).head(num_top_warehouses)

    # Calculate the percentage of credits used and respective cost for the top warehouses
    top_warehouses["Percentage of Credits Used"] = (top_warehouses["Cost of Credits Used by Warehouse"] / total_compute_cost) * 100
    top_warehouses["Respective Cost"] = top_warehouses["Cost of Credits Used by Warehouse"]

    # Display the table of top warehouses by usage in the first column
    col1.title("Top Warehouses by Usage")
    col1.table(top_warehouses)

    # Display the bar graph for top warehouses by usage in the second column
    col2.title("Top Warehouses by Usage (Graph)")
    col2.bar_chart(top_warehouses[["Cost of Credits Used by Warehouse", "Percentage of Credits Used", "Respective Cost"]].set_index(top_warehouses.index))

    # ... (rest of the code)
