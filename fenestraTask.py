#!/usr/bin/env python
# coding: utf-8

# In[38]:


from google.cloud import storage
import os
import glob
import pandas as pd
import psycopg2


# Downloaded File Checker and Downloader
# -----------------------

# In[37]:


def download_new_files(bucket_name, local_download_path, downloaded_files_record):
    """
    Downloads new files from a Google Cloud Storage (GCS) bucket to a specified local directory.

    This function interacts with a GCS bucket to identify and download files that have not
    previously been downloaded, as indicated by the 'downloaded_files_record'. 
    It ensures that only new files from the GCS bucket are downloaded, avoiding re-downloading 
    of files already present in the local directory. Once downloaded, the file names are added 
    to the 'downloaded_files_record' to maintain an updated record of all downloaded files.

    The function performs several key operations:
    - Initializes a GCS client.
    - Checks if the specified local download path exists, creating it if necessary.
    - Loads a record of previously downloaded files from 'downloaded_files_record'.
    - Lists all files in the specified GCS bucket and downloads those not in the downloaded record.
    - Updates the downloaded files record with any new files downloaded during the operation.
    - Prints a message if all files from the bucket have already been downloaded.

    Parameters:
    bucket_name (str): The name of the GCS bucket from which to download files.
    local_download_path (str): The local file system path where new files should be downloaded.
    downloaded_files_record (str): Path to a file that maintains a record of all previously 
                                   downloaded files.

    Returns:
    None: The function does not return a value but prints messages to the console indicating 
          the status of file downloads.

    The function relies on two helper functions:
    - load_downloaded_files: Reads the 'downloaded_files_record' file and returns a set of downloaded file names.
    - save_downloaded_files: Updates the 'downloaded_files_record' file with the names of new files downloaded.
    """
    # Initialize Google Cloud Storage client
    client = storage.Client()

    # Ensure the download path exists
    if not os.path.exists(local_download_path):
        os.makedirs(local_download_path)

    # Load the record of downloaded files
    downloaded_files = load_downloaded_files(downloaded_files_record)

    # List files in the bucket
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    # Download new files
    new_files_downloaded = False
    for blob in blobs:
        if blob.name not in downloaded_files:
            blob.download_to_filename(os.path.join(local_download_path, blob.name))
            downloaded_files.add(blob.name)
            print(f"Downloaded new file: {blob.name}")
            new_files_downloaded = True

    # Update the record of downloaded files
    save_downloaded_files(downloaded_files, downloaded_files_record)

    # Check if new files have been downloaded
    if not new_files_downloaded:
        print("All files from the bucket have already been downloaded.")

def load_downloaded_files(file_path):
    """Load the record of downloaded files."""
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return set(file.read().splitlines())
    return set()

def save_downloaded_files(downloaded_files, file_path):
    """Save the record of downloaded files."""
    with open(file_path, 'w') as file:
        for file_name in downloaded_files:
            file.write(file_name + '\n')


# Processed File Checker and Command Generator
# -----------------------

# In[34]:


def process_and_ingest_files(directory_path, processed_files_record, table_name):
    """
    Processes gzipped CSV files from a specified directory and generates PostgreSQL COPY commands
    for data ingestion. The function keeps track of processed files to avoid reprocessing.

    This function iterates over gzipped CSV files in the given directory, generates PostgreSQL COPY
    commands for each unprocessed file, and updates a record of processed files. If a file has been
    processed previously (as recorded in the 'processed_files_record'), it is skipped. This ensures
    efficient data ingestion by processing only new files. After processing, the function updates 
    the record file with the names of newly processed files. If all files in the directory have 
    already been processed, the function notifies the user.

    Parameters:
    directory_path (str): The file path to the directory containing gzipped CSV files.
    processed_files_record (str): The file path to a text file that records the names of 
                                  previously processed files.
    table_name (str): The name of the PostgreSQL table into which the data will be ingested.

    Returns:
    None: The function prints the generated SQL COPY commands to the console and updates the
          'processed_files_record' file. It does not return any value.

    The function checks the 'processed_files_record' file to determine which files have been
    processed in previous runs. It then processes only new files and updates the record
    accordingly. This approach is particularly useful for recurring data ingestion tasks where
    new files are periodically added to the directory.

    Example usage:
    >>> process_and_ingest_files('/path/to/csv/files', '/path/to/processed_record.txt', 'database_table')
    """

    # Load the record of processed files
    if os.path.exists(processed_files_record):
        with open(processed_files_record, 'r') as file:
            processed_files = set(file.read().splitlines())
    else:
        processed_files = set()

    # Flag to track new file processing
    new_files_processed = False

    # Generate the COPY command for each gzipped CSV file
    for file_path in glob.glob(os.path.join(directory_path, '*.csv.gz')):
        file_name = os.path.basename(file_path)
        if file_name not in processed_files:
            command = f"\\copy {table_name} FROM PROGRAM 'gzip -cd {file_path}' WITH (FORMAT csv, HEADER);"
            print(command)
            processed_files.add(file_name)
            new_files_processed = True

    # Update the record of processed files
    with open(processed_files_record, 'w') as file:
        for file_name in processed_files:
            file.write(file_name + '\n')

    # Check if new files have been processed
    if not new_files_processed:
        print("All files have been ingested.")


# In[33]:


# os.path.basename(glob.glob(os.path.join(directory_path, '*.csv.gz'))[0])


# ----------

# In[13]:


def db_operation(operation_type, connection_params, table_name=None, query=None, table_list=None):
    """
    Perform various database operations based on the specified operation type.

    This function establishes a connection to a PostgreSQL database using provided
    connection parameters and executes an operation based on the specified operation type.
    The operations include listing tables, creating a new table, dropping tables,
    showing top rows from a table, and executing a custom query.

    Parameters:
    operation_type (str): The type of database operation to perform. 
                          Valid options include 'list_tables', 'create_table', 
                          'drop_tables', 'show_top_rows', 'custom_query'.
    connection_params (dict): A dictionary containing database connection parameters 
                              such as 'host', 'database', 'user', 'password'.
    table_name (str, optional): Name of the table to be used or created. 
                                Required for 'create_table', 'drop_tables', 'show_top_rows'.
    query (str, optional): Custom SQL query string to be executed. 
                           Required for 'custom_query'.
    table_list (list of str, optional): List of table names to be dropped. 
                                        Required for 'drop_tables'.

    Returns:
    None: This function does not return any value. It performs database operations and 
          prints the results to the console.

    Raises:
    Exception: General exception if any error occurs during the database operations.

    Examples:
    To list all tables in a database:
    >>> db_operation('list_tables', connection_params)

    To create a new table:
    >>> db_operation('create_table', connection_params, table_name='new_table')

    To drop multiple tables:
    >>> db_operation('drop_tables', connection_params, table_list=['table1', 'table2'])

    To show the top 5 rows of a table:
    >>> db_operation('show_top_rows', connection_params, table_name='existing_table')

    To execute a custom SQL query:
    >>> db_operation('custom_query', connection_params, query='SELECT * FROM table_name')
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            if operation_type == 'list_tables':
                # List all table names in the public schema
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                for table in cur.fetchall():
                    print(table[0])

            elif operation_type == 'create_table':
                # Create a new table with a predefined schema
                create_table_query = f"""
                CREATE TABLE {table_name} (
                    Time TEXT NOT NULL,
                    AdvertiserId BIGINT NOT NULL,
                    OrderId BIGINT NOT NULL,
                    LineItemId BIGINT NOT NULL,
                    CreativeId BIGINT,
                    CreativeVersion INT,
                    CreativeSize TEXT,
                    AdUnitId BIGINT,
                    Domain TEXT,
                    CountryId INT,
                    RegionId INT,
                    MetroId INT,
                    CityId INT,
                    BrowserId INT,
                    OSId INT,
                    OSVersion TEXT,
                    TimeUsec2 NUMERIC,
                    KeyPart TEXT,
                    Product TEXT,
                    RequestedAdUnitSizes TEXT,
                    BandwidthGroupId INT,
                    MobileDevice TEXT,
                    IsCompanion BOOLEAN,
                    DeviceCategory TEXT,
                    ActiveViewEligibleImpression BOOLEAN,
                    MobileCarrier TEXT,
                    EstimatedBackfillRevenue NUMERIC,
                    GfpContentId BIGINT,
                    PostalCodeId INT,
                    BandwidthId INT,
                    AudienceSegmentIds TEXT,
                    MobileCapability TEXT,
                    PublisherProvidedID TEXT,
                    VideoPosition TEXT,
                    PodPosition INT,
                    VideoFallbackPosition INT,
                    IsInterstitial BOOLEAN,
                    EventTimeUsec2 NUMERIC,
                    EventKeyPart TEXT,
                    YieldGroupCompanyId TEXT,
                    RequestLanguage TEXT,
                    DealId TEXT,
                    SellerReservePrice NUMERIC,
                    DealType TEXT,
                    AdxAccountId TEXT,
                    Buyer TEXT,
                    Advertiser TEXT,
                    Anonymous BOOLEAN,
                    ImpressionId TEXT
                );
                """
                cur.execute(create_table_query)
                conn.commit()

            elif operation_type == 'drop_tables':
                # Drop multiple tables
                for table in table_list:
                    try:
                        cur.execute(f"DROP TABLE IF EXISTS {table};")
                        print(f"Table '{table}' dropped.")
                        conn.commit()
                    except Exception as e:
                        print(f"Error dropping table {table}: {e}")
            elif operation_type == 'show_top_rows':
                try:
                    cur.execute(f"SELECT * FROM {table_name} LIMIT 5;")
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
                except Exception as e:
                    print(f"Error retrieving rows from table {table_name}: {e}")
            elif operation_type == 'custom_query':
                # Execute a custom query
                try:
                    cur.execute(query)
                    if cur.description:
                        # Fetch and print query results if there are any
                        rows = cur.fetchall()
                        for row in rows:
                            print(row)
                    else:
                        # Commit changes if it's an action query (INSERT, UPDATE, DELETE)
                        conn.commit()
                except Exception as e:
                    print(f"Error executing custom query: {e}")

