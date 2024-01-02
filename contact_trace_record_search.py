import os
import streamlit as st
import pandas as pd
import boto3
import json
import os
import glob

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
bucket_name = st.text_input(
    'S3 Bucket Name', value=bucket_name)

folder_path = 'CTRs'

# contacts
load_button = st.button('Load')
if load_button:
    with st.spinner('Loading......'):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        obj_cnt = 0
        no_file_found = True
        for obj in bucket.objects.all():
            if obj.key.endswith('.csv'):
                df = pd.read_csv(obj.get()['Body'])
                filename = obj.key.split('/')[-1]
                file_path = os.path.join(folder_path, filename)
                df.to_csv(file_path, index=False)
                no_file_found = False
                obj_cnt += 1

        st.write("Files in S3 Bucket:"+str(obj_cnt))
        if no_file_found:
            st.write("No CSV file found.")

        st.stop()

visualize_button = st.button('Visualize')
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

    frame = pd.concat(li, axis=0, ignore_index=True)
    frame = frame.sort_values(by='date').reset_index(drop=True)
    st.dataframe(frame)

    # Daily aggregate
    daily_df = frame.groupby('date').agg({'agentinteractionduration_seconds': 'sum',
                                          'contactduration_seconds': 'sum'}).reset_index()
    daily_df = daily_df.sort_values(by='date')

    st.bar_chart(daily_df, x='date')

    st.write(daily_df)
