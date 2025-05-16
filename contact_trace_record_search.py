import os
import streamlit as st
import pandas as pd
import boto3
import json
import glob
import csv
from datetime import datetime
from io import BytesIO

# 创建 AWS 客户端
s3_client = boto3.client('s3')
connect_client = boto3.client("connect")

def convert_to_numeric(val):
    """将值转换为数字，如果为'None'则返回0"""
    if val == 'None':
        return 0
    return int(val)

def get_agent_interaction_duration(data):
    """从 agent 数据中提取 agentinteractionduration 值"""
    parts = data.split(', ')
    for part in parts:
        key_value = part.split('=')
        if key_value[0] == 'agentinteractionduration':
            return convert_to_numeric(key_value[1])

def get_after_contact_work_duration(data):
    """从 agent 数据中提取 aftercontactworkduration 值"""
    parts = data.split(', ')
    for part in parts:
        key_value = part.split('=')
        if key_value[0] == 'aftercontactworkduration':
            return convert_to_numeric(key_value[1])

def detect_file_type(file_name):
    """根据文件扩展名检测文件类型"""
    if file_name.lower().endswith('.csv'):
        return 'csv'
    elif file_name.lower().endswith('.parquet'):
        return 'parquet'
    elif file_name.lower().endswith('.json'):
        return 'json'
    else:
        return None

def search_ctr_data(folder_path, contact_id=None):
    """
    加载 ctr_data.csv 文件并搜索匹配 contact_id 的记录
    
    Args:
        folder_path: 包含 ctr_data.csv 的文件夹路径
        contact_id: 要搜索的联系 ID
        
    Returns:
        包含匹配记录的 DataFrame，如果 contact_id 为 None，则返回所有记录
    """
    ctr_file_path = os.path.join(folder_path, "ctr_data.csv")
    
    if not os.path.exists(ctr_file_path):
        st.warning(f"文件未找到: {ctr_file_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(ctr_file_path)
        if contact_id and not df.empty:
            if 'contactid' in df.columns:
                return df[df['contactid'] == contact_id]
            else:
                st.warning("数据中未找到'contactid'列")
                return df
        return df
    except Exception as e:
        st.error(f"读取 {ctr_file_path} 时出错: {e}")
        return pd.DataFrame()

def save_dataframe_to_csv(df, output_dir, file_name=None, add_timestamp=False, 
                         encoding='utf-8', sep=',', index=False, 
                         na_rep='', date_format='%Y-%m-%d', 
                         float_format='%.2f', quoting=csv.QUOTE_MINIMAL):
    """
    将 DataFrame 保存为 CSV 文件
    """
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
    """
    根据类型和前缀从 S3 加载文件
    """
    obj_cnt = 0
    no_file_found = True
    all_dfs = []  # 存储所有数据帧的列表
    
    # 使用分页器处理大量对象
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # 如果提供了 folder_prefix，使用它过滤对象
    if folder_prefix:
        pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
    else:
        pages = paginator.paginate(Bucket=bucket_name)
    
    # 处理每个对象
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
                    
                    # 添加源文件信息
                    df['sourcefile'] = filename
                    all_dfs.append(df)
                    
                    no_file_found = False
                    obj_cnt += 1
                    st.write(f"已处理 {object_key}")
                except Exception as e:
                    st.error(f"处理 {object_key} 时出错: {e}")
    
    # 将所有数据帧合并为一个
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        # 保存合并的数据帧
        save_dataframe_to_csv(combined_df, folder_path, file_name="ctr_data")
    else:
        combined_df = pd.DataFrame()
    
    return obj_cnt, no_file_found


# 设置页面配置
st.set_page_config(
    page_title="Amazon Connect Contact Search Plus Tool!", layout="wide")

# 应用标题
st.header("Amazon Connect Contact Search Plus Tool!")

# 读取存储的 S3 桶名称
bucket_name = ''
if os.path.exists('s3bucket.json'):
    with open('s3bucket.json') as f:
        json_data = json.load(f)
        bucket_name = json_data['BucketName']

# Connect 配置
s3_path = st.text_input('S3 Bucket Name', value=bucket_name)

# 解析 S3 路径
folder_prefix = ''
if s3_path:
    if "://" in s3_path:
        parts = s3_path.split("://")
        if len(parts) > 1:
            bucket_name = parts[1].split("/")[0]
            folder_prefix = "/".join(parts[1].split("/")[1:]) if len(parts[1].split("/")) > 1 else ''
            st.write(f"解析的桶: {bucket_name}, 前缀: {folder_prefix}")
    else:
        bucket_name = s3_path

# 创建存储文件夹
folder_path = 'CTRs'
if not os.path.exists(folder_path):
    os.makedirs(folder_path, exist_ok=True)

# 创建两列布局
col1, col2 = st.columns(2)

# 第一列：加载控件
with col1:
    load_button = st.button('Load')
    if load_button:
        with st.spinner('从 S3 加载文件中...'):
            st.write(f"从桶加载: {bucket_name}, 前缀: {folder_prefix if folder_prefix else 'None'}")
            obj_cnt, no_file_found = load_files_from_s3(bucket_name, folder_prefix, folder_path)
            st.write(f"从 S3 桶加载的文件数: {obj_cnt}")
            if no_file_found:
                st.write("未找到文件。")

# 第二列：可视化控件
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

            # 每日聚合
            daily_df = frame.groupby('date').agg({'agentinteractionduration_seconds': 'sum',
                                                'contactduration_seconds': 'sum'}).reset_index()
            daily_df = daily_df.sort_values(by='date')

            st.bar_chart(daily_df, x='date')
            st.write(daily_df)
        else:
            st.write("未找到可视化的 CSV 文件。")

# 搜索功能
contact_id = st.text_input('Contact Id')
search_button = st.button('Search')
if search_button:
    # 使用搜索函数在 ctr_data.csv 中查找匹配记录
    result_df = search_ctr_data(folder_path, contact_id)
    
    if not result_df.empty:
        st.write(f"找到 {len(result_df)} 条匹配记录:")
        st.dataframe(result_df)
    else:
        st.write("未找到匹配记录。")