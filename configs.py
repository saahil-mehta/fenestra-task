import os

with open("pass.txt", 'r') as file:
    password = file.read().strip()

# PostgreSQL Database Configurations
DB_CONFIG = {
    'host': '34.78.210.253',
    'database': 'saahil',
    'user': 'saahil',
    'password': password
}

current_working_directory = os.getcwd()
# Google Cloud Storage Configurations
TASK_CONFIG = {
    'bucket_name': 'fst-python-case-study',
    'local_download_path': os.path.join(current_working_directory, 'downloaded_files'),
    'downloaded_files_record': os.path.join(current_working_directory, 'downloaded_files_record.txt'),
    'service_account_key_path': 'scratch-253214-e290c1ac61f2.json',
    'processed_files_record' : os.path.join(current_working_directory, 'processed_files_record.txt')
}

# Other configurations can be added here
