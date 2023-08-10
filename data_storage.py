import snowflake.connector
import psycopg2

class DataStorage:
    def __init__(self):
        self.sfconn = None
        self.pgconn = None
        self.sfcursor = None
        self.pgcursor = None

    def connect_to_snowflake(self):
        self.sfconn = snowflake.connector.connect(
            account='ufa89233.us-east-1',
            user='SCO',
            password='Sco@12345',
            role="SCO_ROLE",
            warehouse='B360I_DS_DEV',
            database='SCO_DB',
            schema='TEST_SCO'
        )
        self.sfcursor = self.sfconn.cursor()

    def connect_to_postgres(self):
        self.pgconn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='warehouse_insights',
            user='postgres',
            password='Figo@1972'
        )
        self.pgcursor = self.pgconn.cursor()

    def create_postgres_table(self):
        self.pgcursor.execute("""
            CREATE TABLE IF NOT EXISTS warehouse_metering_history_test_b (
                START_TIME TIMESTAMPTZ,
                END_TIME TIMESTAMPTZ,
                WAREHOUSE_ID INT,
                WAREHOUSE_NAME VARCHAR(100),
                CREDITS_USED FLOAT,
                CREDITS_USED_COMPUTE FLOAT,
                CREDITS_USED_CLOUD_SERVICES FLOAT
            );
        """)
        self.pgconn.commit()

    def create_query_history_into_postgres(self):
            

# Create the PostgreSQL table if it doesn't exist

        self.pgcursor.execute("""
            CREATE TABLE IF NOT EXISTS query_history (
                QUERY_ID VARCHAR(10485760) NULL,
                QUERY_TEXT VARCHAR(10485760) NULL,
                DATABASE_ID NUMERIC(38,0) NULL,
                DATABASE_NAME VARCHAR(10485760) NULL,
                SCHEMA_ID NUMERIC(38,0) NULL,
                SCHEMA_NAME VARCHAR(10485760) NULL,
                QUERY_TYPE VARCHAR(10485760) NULL,
                SESSION_ID NUMERIC(38,0) NULL,
                USER_NAME VARCHAR(10485760) NULL,
                ROLE_NAME VARCHAR(10485760) NULL,
                WAREHOUSE_ID NUMERIC(38,0) NULL,
                WAREHOUSE_NAME VARCHAR(10485760) NULL,
                WAREHOUSE_SIZE VARCHAR(10485760) NULL,
                WAREHOUSE_TYPE VARCHAR(10485760) NULL,
                CLUSTER_NUMBER NUMERIC(38,0) NULL,
                QUERY_TAG VARCHAR(10485760) NULL,
                EXECUTION_STATUS VARCHAR(10485760) NULL,
                ERROR_CODE VARCHAR(10485760) NULL,
                ERROR_MESSAGE VARCHAR(10485760) NULL,
                START_TIME TIMESTAMP NULL,
                END_TIME TIMESTAMP NULL,
                TOTAL_ELAPSED_TIME NUMERIC(38,0) NULL,
                BYTES_SCANNED NUMERIC(38,0) NULL,
                PERCENTAGE_SCANNED_FROM_CACHE REAL NULL,
                BYTES_WRITTEN NUMERIC(38,0) NULL,
                BYTES_WRITTEN_TO_RESULT NUMERIC(38,0) NULL,
                BYTES_READ_FROM_RESULT NUMERIC(38,0) NULL,
                ROWS_PRODUCED NUMERIC(38,0) NULL,
                ROWS_INSERTED NUMERIC(38,0) NULL,
                ROWS_UPDATED NUMERIC(38,0) NULL,
                ROWS_DELETED NUMERIC(38,0) NULL,
                ROWS_UNLOADED NUMERIC(38,0) NULL,
                BYTES_DELETED NUMERIC(38,0) NULL,
                PARTITIONS_SCANNED NUMERIC(38,0) NULL,
                PARTITIONS_TOTAL NUMERIC(38,0) NULL,
                BYTES_SPILLED_TO_LOCAL_STORAGE NUMERIC(38,0) NULL,
                BYTES_SPILLED_TO_REMOTE_STORAGE NUMERIC(38,0) NULL,
                BYTES_SENT_OVER_THE_NETWORK NUMERIC(38,0) NULL,
                COMPILATION_TIME NUMERIC(38,0) NULL,
                EXECUTION_TIME NUMERIC(38,0) NULL,
                QUEUED_PROVISIONING_TIME NUMERIC(38,0) NULL,
                QUEUED_REPAIR_TIME NUMERIC(38,0) NULL,
                QUEUED_OVERLOAD_TIME NUMERIC(38,0) NULL,
                TRANSACTION_BLOCKED_TIME NUMERIC(38,0) NULL,
                OUTBOUND_DATA_TRANSFER_CLOUD VARCHAR(10485760) NULL,
                OUTBOUND_DATA_TRANSFER_REGION VARCHAR(10485760) NULL,
                OUTBOUND_DATA_TRANSFER_BYTES NUMERIC(38,0) NULL,
                INBOUND_DATA_TRANSFER_CLOUD VARCHAR(10485760) NULL,
                INBOUND_DATA_TRANSFER_REGION VARCHAR(10485760) NULL,
                INBOUND_DATA_TRANSFER_BYTES NUMERIC(38,0) NULL,
                LIST_EXTERNAL_FILES_TIME NUMERIC(38,0) NULL,
                CREDITS_USED_CLOUD_SERVICES REAL NULL,
                RELEASE_VERSION VARCHAR(10485760) NULL,
                EXTERNAL_FUNCTION_TOTAL_INVOCATIONS NUMERIC(38,0) NULL,
                EXTERNAL_FUNCTION_TOTAL_SENT_ROWS NUMERIC(38,0) NULL,
                EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS NUMERIC(38,0) NULL,
                EXTERNAL_FUNCTION_TOTAL_SENT_BYTES NUMERIC(38,0) NULL,
                EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES NUMERIC(38,0) NULL,
                QUERY_LOAD_PERCENT NUMERIC(38,0) NULL,
                IS_CLIENT_GENERATED_STATEMENT BOOLEAN NULL,
                QUERY_ACCELERATION_BYTES_SCANNED NUMERIC(38,0) NULL,
                QUERY_ACCELERATION_PARTITIONS_SCANNED NUMERIC(38,0) NULL,
                QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR NUMERIC(38,0) NULL,
                TRANSACTION_ID NUMERIC(38,0) NULL,
                CHILD_QUERIES_WAIT_TIME NUMERIC(38,0) NULL,
                ROLE_TYPE VARCHAR(10485760) NULL
            );
        """)

        self.pgconn.commit()
    
    def insert_query_history_into_postgres(self, data):
        self.pgcursor.executemany("""
            INSERT INTO query_history (
                QUERY_ID, QUERY_TEXT, DATABASE_ID, DATABASE_NAME, SCHEMA_ID, SCHEMA_NAME, QUERY_TYPE, SESSION_ID, USER_NAME,
                ROLE_NAME, WAREHOUSE_ID, WAREHOUSE_NAME, WAREHOUSE_SIZE, WAREHOUSE_TYPE, CLUSTER_NUMBER, QUERY_TAG,
                EXECUTION_STATUS, ERROR_CODE, ERROR_MESSAGE, START_TIME, END_TIME, TOTAL_ELAPSED_TIME, BYTES_SCANNED,
                PERCENTAGE_SCANNED_FROM_CACHE, BYTES_WRITTEN, BYTES_WRITTEN_TO_RESULT, BYTES_READ_FROM_RESULT,
                ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_UNLOADED, BYTES_DELETED, PARTITIONS_SCANNED,
                PARTITIONS_TOTAL, BYTES_SPILLED_TO_LOCAL_STORAGE, BYTES_SPILLED_TO_REMOTE_STORAGE,
                BYTES_SENT_OVER_THE_NETWORK, COMPILATION_TIME, EXECUTION_TIME, QUEUED_PROVISIONING_TIME, QUEUED_REPAIR_TIME,
                QUEUED_OVERLOAD_TIME, TRANSACTION_BLOCKED_TIME, OUTBOUND_DATA_TRANSFER_CLOUD, OUTBOUND_DATA_TRANSFER_REGION,
                OUTBOUND_DATA_TRANSFER_BYTES, INBOUND_DATA_TRANSFER_CLOUD, INBOUND_DATA_TRANSFER_REGION,
                INBOUND_DATA_TRANSFER_BYTES, LIST_EXTERNAL_FILES_TIME, CREDITS_USED_CLOUD_SERVICES, RELEASE_VERSION,
                EXTERNAL_FUNCTION_TOTAL_INVOCATIONS, EXTERNAL_FUNCTION_TOTAL_SENT_ROWS, EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,
                EXTERNAL_FUNCTION_TOTAL_SENT_BYTES, EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES, QUERY_LOAD_PERCENT,
                IS_CLIENT_GENERATED_STATEMENT, QUERY_ACCELERATION_BYTES_SCANNED, QUERY_ACCELERATION_PARTITIONS_SCANNED,
                QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR, TRANSACTION_ID, CHILD_QUERIES_WAIT_TIME, ROLE_TYPE
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """, data)
        self.pgconn.commit()
    
    def fetch_query_history_from_postgres(self):
        self.pgcursor.execute("SELECT * FROM query_history;")
        return self.pgcursor.fetchall()

    def fetch_data_from_snowflake(self):
        self.sfcursor.execute("select * from SCO_DB.TEST_SCO.WAREHOUSE_METERING_HISTORY_TEST;")
        return self.sfcursor.fetchall()

    def insert_data_into_postgres(self, data):
        self.pgcursor.executemany("""
            INSERT INTO warehouse_metering_history_test (
                START_TIME, END_TIME, WAREHOUSE_ID, WAREHOUSE_NAME, CREDITS_USED, CREDITS_USED_COMPUTE, CREDITS_USED_CLOUD_SERVICES
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, data)
        self.pgconn.commit()
   
    def fetch_data_from_postgres(self):
        self.pgcursor.execute("SELECT * FROM warehouse_metering_history_b;")
        return self.pgcursor.fetchall()

    def close_connections(self):
        if self.sfcursor:
            self.sfcursor.close()
        if self.sfconn:
            self.sfconn.close()
        if self.pgcursor:
            self.pgcursor.close()
        if self.pgconn:
            self.pgconn.close()

if __name__ == "__main__":
    data_storage = DataStorage()

    print("Choose where to store data:")
    print("1. PostgreSQL")
    print("2. Snowflake")
    choice = int(input("Enter your choice (1 or 2): "))

    if choice == 1:
        data_storage.connect_to_postgres()
        data_storage.create_postgres_table()
        data = data_storage.fetch_data_from_snowflake()
        data_storage.insert_data_into_postgres(data)
        data_storage.insert_query_history_into_postgres(data)
        
        # Fetch the inserted query history data from PostgreSQL
        inserted_query_history = data_storage.fetch_query_history_from_postgres()
        
    elif choice == 2:
        data_storage.connect_to_snowflake()
        data = data_storage.fetch_data_from_snowflake()
        data_storage.insert_data_into_snowflake(data)
    else:
        print("Invalid choice. Please choose 1 or 2.")

    data_storage.close_connections()
