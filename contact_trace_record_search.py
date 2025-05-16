import os
import streamlit as st
import pandas as pd
import boto3
import json
import os
import glob
import csv
from datetime import datetime
import io
from io import BytesIO

connect_client = boto3.client("connect")


def get_agent_interaction_duration(data):
    parts = data.split(', ')
    for part in parts:
        key_value = part.split('=')
        if key_value[0] == 'agentinteractionduration':
            return convert_to_numeric(key_value[1])


def get_after_contact_work_duration(data):
    parts = data.split(', ')
    for part in parts:
        key_value = part.split('=')
        if key_value[0] == 'aftercontactworkduration':
            return convert_to_numeric(key_value[1])


def convert_to_numeric(val):
    if val == 'None':
        return 0
    return int(val)


def download_directory(bucket_name, prefix, local_dir):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            object_key = obj['Key']
            local_file_path = os.path.join(
                local_dir, os.path.relpath(object_key, prefix))
            try:
                s3_client.download_file(
                    bucket_name, object_key, local_file_path)
                st.write(f'Downloaded {object_key} to {local_file_path}')
            except Exception as e:
                st.write(f'Error downloading {object_key}: {e}')


def detect_file_type(file_name):
    """Detect file type based on extension."""
    if file_name.lower().endswith('.csv'):
        return 'csv'
    elif file_name.lower().endswith('.parquet'):
        return 'parquet'
    elif file_name.lower().endswith('.json'):
        return 'json'
    else:
        return None

def read_parquet_files(folder_path):
    dfs = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.parquet'):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_parquet(file_path)
            df['sourcefile'] = file_name
            dfs.append(df)

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    return pd.DataFrame()

def search_ctr_data(folder_path, contact_id=None):
    """
    Load ctr_data.csv file and search for records matching the contact_id.
    
    Args:
        folder_path: Path to the folder containing ctr_data.csv
        contact_id: Contact ID to search for
        
    Returns:
        DataFrame with matching records or all records if contact_id is None
    """
    ctr_file_path = os.path.join(folder_path, "ctr_data.csv")
    
    if not os.path.exists(ctr_file_path):
        st.warning(f"File not found: {ctr_file_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(ctr_file_path)
        if contact_id and not df.empty:
            if 'contactid' in df.columns:
                return df[df['contactid'] == contact_id]
            else:
                st.warning("Column 'contactid' not found in the data")
                return df
        return df
    except Exception as e:
        st.error(f"Error reading {ctr_file_path}: {e}")
        return pd.DataFrame()

def save_dataframe_to_csv(df, output_dir, file_name=None, add_timestamp=False, 
                         encoding='utf-8', sep=',', index=False, 
                         na_rep='', date_format='%Y-%m-%d', 
                         float_format='%.2f', quoting=csv.QUOTE_MINIMAL):
    try:
        # 创建输出目录（如果不存在）
        os.makedirs(output_dir, exist_ok=True)
        
        # 设置默认文件名
        if file_name is None:
            file_name = 'data'
            
        # 添加时间戳到文件名（如果需要）
        if add_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"{file_name}_{timestamp}"
        
        # 确保文件名有.csv扩展名
        if not file_name.endswith('.csv'):
            file_name = f"{file_name}.csv"
            
        # 完整的文件路径
        file_path = os.path.join(output_dir, file_name)
        
        # 保存DataFrame为CSV文件
        df.to_csv(
            path_or_buf=file_path,
            sep=sep,
            index=index,
            encoding=encoding,
            na_rep=na_rep,
            float_format=float_format,
            date_format=date_format,
            quoting=quoting
        )
        
        print(f"CSV文件已保存至: {file_path}")
        return file_path
        
    except Exception as e:
        print(f"保存CSV文件时出错: {str(e)}")
        raise
    
def load_files_from_s3(bucket_name, folder_prefix, folder_path):
    """Load files from S3 based on their type and prefix."""
    obj_cnt = 0
    no_file_found = True
    all_dfs = []  # List to store all dataframes
    
    # Use paginator to handle large number of objects
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # If folder_prefix is provided, use it to filter objects
    if folder_prefix:
        pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
    else:
        pages = paginator.paginate(Bucket=bucket_name)
    
    # Process each object
    for page in pages:
        for obj in page.get('Contents', []):
            object_key = obj['Key']
            file_type = detect_file_type(object_key)
            
            if file_type:
                filename = os.path.basename(object_key)
                
                try:
                    s3_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                    file_content = s3_obj['Body'].read()
                    
                    if file_type == 'csv':
                        df = pd.read_csv(BytesIO(file_content))
                    elif file_type == 'parquet':
                        df = pd.read_parquet(BytesIO(file_content))
                    elif file_type == 'json':
                        df = pd.read_json(BytesIO(file_content))
                    
                    # Add source file information
                    df['sourcefile'] = filename
                    all_dfs.append(df)
                    
                    no_file_found = False
                    obj_cnt += 1
                    st.write(f"Processed {object_key}")
                except Exception as e:
                    st.error(f"Error processing {object_key}: {e}")
    
    # Combine all dataframes into one
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        # Save the combined dataframe
        save_dataframe_to_csv(combined_df, folder_path, file_name="ctr_data")
    else:
        combined_df = pd.DataFrame()
    
    return obj_cnt, no_file_found


s3_client = boto3.client('s3')

st.set_page_config(
    page_title="Amazon Connect Contact Search Plus Tool!", layout="wide")

# app title
st.header(f"Amazon Connect Contact Search Plus Tool!")

bucket_name = ''
if os.path.exists('s3bucket.json'):
    with open('s3bucket.json') as f:
        json_data = json.load(f)
        bucket_name = json_data['BucketName']

# connect configuration
s3_path = st.text_input(
    'S3 Bucket Name', value=bucket_name)

folder_prefix = ''
if s3_path:
    if "://" in s3_path:
        parts = s3_path.split("://")
        if len(parts) > 1:
            bucket_name = parts[1].split("/")[0]
            folder_prefix = "/".join(parts[1].split("/")[1:]) if len(parts[1].split("/")) > 1 else ''
            st.write(f"Parsed bucket: {bucket_name}, prefix: {folder_prefix}")
    else:
        bucket_name = s3_path

folder_path = 'CTRs'

if not os.path.exists(folder_path):
    os.makedirs(folder_path, exist_ok=True)

col1, col2 = st.columns(2)

# contacts
with col1:
    load_button = st.button('Load')
    if load_button:
        with st.spinner('Loading files from S3...'):
            st.write(f"Loading from bucket: {bucket_name}, prefix: {folder_prefix if folder_prefix else 'None'}")
            obj_cnt, no_file_found = load_files_from_s3(bucket_name, folder_prefix, folder_path)
            st.write(f"Files loaded from S3 Bucket: {obj_cnt}")
            if no_file_found:
                st.write("No files found.")

with col2:
    visualize_button = st.button('Visualize CSV')
    if visualize_button:
        all_files = glob.glob(os.path.join(folder_path, "*.csv"))
        li = []
        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0)
            df = df[df['channel'] == 'VOICE']
            not_null = df['agent'].notna()
            df['agentinteractionduration_seconds'] = df.loc[not_null,
                                                            'agent'].apply(get_agent_interaction_duration)
            df['aftercontactworkduration_seconds'] = df.loc[not_null,
                                                            'agent'].apply(get_after_contact_work_duration)

            df['initiationtimestampnew'] = pd.to_datetime(
                df['initiationtimestamp'])
            df['disconnecttimestampnew'] = pd.to_datetime(
                df['disconnecttimestamp'])
            df['date'] = pd.to_datetime(
                df['disconnecttimestamp']).dt.strftime('%Y-%m-%d')

            df['contactduration'] = df['disconnecttimestampnew'] - \
                df['initiationtimestampnew']
            df['contactduration_seconds'] = df['contactduration'].dt.total_seconds()

            df = df[['contactid', 'channel', 'initiationtimestamp', 'initiationtimestampnew', 'connectedtosystemtimestamp', 'date',
                    'agentinteractionduration_seconds', 'aftercontactworkduration_seconds', 'disconnecttimestamp', 'disconnecttimestampnew', 'contactduration_seconds']]
            li.append(df)

        if li:
            frame = pd.concat(li, axis=0, ignore_index=True)
            frame = frame.sort_values(by='date').reset_index(drop=True)
            st.dataframe(frame)

            # Daily aggregate
            daily_df = frame.groupby('date').agg({'agentinteractionduration_seconds': 'sum',
                                                'contactduration_seconds': 'sum'}).reset_index()
            daily_df = daily_df.sort_values(by='date')

            st.bar_chart(daily_df, x='date')
            st.write(daily_df)
        else:
            st.write("No CSV files found to visualize.")

contact_id = st.text_input('Contact Id')
search_button = st.button('Search')
if search_button:
    # Use the new search function to find matching records in ctr_data.csv
    result_df = search_ctr_data(folder_path, contact_id)
    
    if not result_df.empty:
        st.write(f"Found {len(result_df)} matching records:")
        st.dataframe(result_df)
    else:
        st.write("No matching records found.")