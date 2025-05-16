import os
import streamlit as st
import pandas as pd
import boto3
import json
import csv
from datetime import datetime
from io import BytesIO
import datetime
import numpy as np
import altair as alt
from dateutil.parser import parse

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
                    
                    # 添加日志消息到session_state
                    st.session_state.log_messages.append(f"已处理 {object_key}")
                        
                except Exception as e:
                    error_msg = f"处理 {object_key} 时出错: {e}"
                    st.session_state.log_messages.append(f"❌ {error_msg}")
    
    # 将所有数据帧合并为一个
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        # 保存合并的数据帧
        save_dataframe_to_csv(combined_df, folder_path, file_name="ctr_data")
        st.session_state.log_messages.append(f"✅ 已保存合并数据到 {os.path.join(folder_path, 'ctr_data.csv')}")
    else:
        combined_df = pd.DataFrame()
    
    return obj_cnt, no_file_found

def contact_delay_analyzer(file_path=None):
    """
    创建Streamlit应用来分析和可视化联系记录数据。
    
    功能：
    1. 根据contactid筛选数据，选择lastupdatetimestamp最新时间的记录
    2. 计算connectedtosystemtimestamp和initiationtimestamp之间的时间差（秒）
    3. 允许用户筛选大于特定时间差的记录并统计数量
    
    参数：
    - csv_data: 字符串，CSV数据内容
    - file_path: 字符串，CSV文件路径（如果csv_data为None）
    
    返回值：
    - 无，直接在Streamlit界面上显示结果
    """
    # 当用户点击下载按钮时，保持分析状态
    if 'show_analysis' not in st.session_state:
        st.session_state.show_analysis = True
    st.title("联系记录延迟分析工具")
    
    # 读取数据
    df = None
    if file_path:
        df = pd.read_csv(file_path)
    else:
        st.warning(f"数据中未找到{file_path}")
    
    if df is None:
        st.info("请上传CSV文件或提供数据")
        return
    
    # 数据预处理
    st.subheader("数据预处理")
    with st.expander("查看原始数据", expanded=False):
        st.dataframe(df)
    
    # 将时间戳列转换为日期时间格式
    for col in ['connectedtosystemtimestamp', 'initiationtimestamp', 'lastupdatetimestamp']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    # 计算时间差（秒）
    df['delay_seconds'] = (df['connectedtosystemtimestamp'] - df['initiationtimestamp']).dt.total_seconds()
    
    # 对于每个contactid，保留lastupdatetimestamp最新的记录
    st.write("按contactid筛选并保留最新更新的记录:")
    latest_records = df.sort_values('lastupdatetimestamp').groupby('contactid').last().reset_index()
    
    # 显示处理后的数据
    with st.expander("查看筛选后的数据", expanded=True):
        st.dataframe(latest_records[['contactid', 'initiationtimestamp', 
                                    'connectedtosystemtimestamp', 'delay_seconds', 
                                    'lastupdatetimestamp']])
    
    # 基本统计信息
    st.subheader("延迟时间统计信息")
    delay_stats = latest_records['delay_seconds'].describe()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("平均延迟时间（秒）", f"{delay_stats['mean']:.2f}")
        st.metric("最大延迟时间（秒）", f"{delay_stats['max']:.2f}")
        st.metric("总记录数", len(latest_records))
    
    with col2:
        st.metric("中位数延迟时间（秒）", f"{delay_stats['50%']:.2f}")
        st.metric("最小延迟时间（秒）", f"{delay_stats['min']:.2f}")
        st.metric("联系ID数量", latest_records['contactid'].nunique())
    
    # 创建延迟时间分布的直方图 (使用Altair)
    st.subheader("延迟时间分布")
    
    # 为了创建直方图，我们需要使用altair
    chart = alt.Chart(latest_records).mark_bar().encode(
        alt.X('delay_seconds:Q', bin=alt.Bin(maxbins=20), title='延迟时间（秒）'),
        alt.Y('count():Q', title='数量')
    ).properties(
        title='连接系统延迟时间分布（秒）'
    )
    st.altair_chart(chart, use_container_width=True)
    
    # 交互式时间差筛选
    st.subheader("延迟时间筛选分析")
    min_delay = float(latest_records['delay_seconds'].min())
    max_delay = float(latest_records['delay_seconds'].max())
    
    # 滑动条用于选择延迟阈值
    delay_threshold = st.slider(
        "选择延迟阈值（秒）",
        min_value=min_delay,
        max_value=max_delay,
        value=min_delay,
        step=0.1
    )
    
    # 筛选大于阈值的记录
    filtered_records = latest_records[latest_records['delay_seconds'] > delay_threshold]
    filtered_count = len(filtered_records)
    total_count = len(latest_records)
    percentage = (filtered_count / total_count * 100) if total_count > 0 else 0
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("大于阈值的记录数", filtered_count)
    with col2:
        st.metric("占总记录百分比", f"{percentage:.2f}%")
    
    # 显示筛选后的记录
    if not filtered_records.empty:
        with st.expander("查看大于阈值的记录详情", expanded=True):
            st.dataframe(filtered_records[['contactid', 'initiationtimestamp', 
                                          'connectedtosystemtimestamp', 'delay_seconds', 
                                          'channel', 'lastupdatetimestamp']])
    
    # 按渠道分析延迟
    if 'channel' in latest_records.columns:
        st.subheader("不同渠道延迟时间分析")
        
        # 按渠道分组的统计信息
        channel_stats = latest_records.groupby('channel')['delay_seconds'].agg(['mean', 'median', 'count']).reset_index()
        
        # 渠道延迟条形图 (使用Streamlit的内置图表)
        st.bar_chart(
            data=channel_stats,
            x='channel',
            y='mean',
            use_container_width=True
        )
        
        # 显示渠道统计数据表格
        st.write("各渠道延迟统计：")
        st.dataframe(channel_stats)
    
    # 时间序列分析
    st.subheader("延迟时间变化趋势")
    
    # 将日期提取为新列用于分组
    latest_records['date'] = latest_records['initiationtimestamp'].dt.date
    
    # 按日期分组计算平均延迟时间
    daily_stats = latest_records.groupby('date')['delay_seconds'].agg(['mean', 'count']).reset_index()
    daily_stats['date'] = pd.to_datetime(daily_stats['date'])
    
    # 创建时间序列图 (使用Streamlit的内置图表)
    st.line_chart(
        data=daily_stats,
        x='date',
        y='mean',
        use_container_width=True
    )
    
    # 显示日期统计数据表格
    st.write("每日延迟统计：")
    st.dataframe(daily_stats)
    
    # 下载处理后的数据
    st.subheader("数据导出")
    
    # 将处理后的数据提供给用户下载
    csv = latest_records.to_csv(index=False).encode('utf-8')
    
    # 使用回调函数确保下载后保持分析状态
    def maintain_state():
        st.session_state.show_analysis = True
    
    st.download_button(
        label="下载筛选后的数据",
        data=csv,
        file_name="contact_delay_analysis.csv",
        mime="text/csv",
        on_click=maintain_state
    )
    
    # 如果有大于阈值的记录，也提供这部分数据的下载
    if not filtered_records.empty:
        filtered_csv = filtered_records.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="下载大于阈值的记录",
            data=filtered_csv,
            file_name=f"contacts_with_delay_over_{delay_threshold}s.csv",
            mime="text/csv",
            on_click=maintain_state
        )

# 设置页面配置
st.set_page_config(
    page_title="Amazon Connect Contact Search Plus Tool!", layout="wide")

# 添加自定义CSS样式
st.markdown("""
<style>
.scrollable-container {
    max-height: 300px;
    overflow-y: auto;
    border: 1px solid #ddd;
    padding: 10px;
    border-radius: 5px;
    background-color: #f9f9f9;
    margin-bottom: 20px;
}
</style>
""", unsafe_allow_html=True)

# 应用标题
st.header("Amazon Connect Contact Search Plus Tool!")

# 初始化 session_state 以保存输入值
if 's3_path' not in st.session_state:
    # 读取存储的 S3 桶名称
    bucket_name = ''
    if os.path.exists('s3bucket.json'):
        with open('s3bucket.json') as f:
            json_data = json.load(f)
            bucket_name = json_data['BucketName']
    st.session_state.s3_path = bucket_name

# Connect 配置
s3_path = st.text_input('S3 Bucket Name', value=st.session_state.s3_path, key='s3_path_input')
st.session_state.s3_path = s3_path

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

# 初始化日志消息列表
if 'log_messages' not in st.session_state:
    st.session_state.log_messages = []

# 创建一个固定位置的日志容器
log_container = st.container()

# Load 按钮
load_button = st.button('Load')
if load_button:
    with st.spinner('从 S3 加载文件中...'):
        # 清空之前的日志
        st.session_state.log_messages = []
        st.session_state.log_messages.append(f"从桶加载: {bucket_name}, 前缀: {folder_prefix if folder_prefix else 'None'}")
        
        # 调用加载函数
        obj_cnt, no_file_found = load_files_from_s3(bucket_name, folder_prefix, folder_path)
        
        # 显示结果摘要
        if no_file_found:
            st.session_state.log_messages.append("### 未找到文件。")
        else:
            st.session_state.log_messages.append(f"### 从 S3 桶加载的文件数: {obj_cnt}")

# 在固定的容器中显示日志
with log_container:
    if st.session_state.log_messages:
        # 使用HTML创建固定高度的滚动区域
        log_html = "<div class='scrollable-log'>" + "<br>".join(st.session_state.log_messages) + "</div>"
        st.markdown(log_html, unsafe_allow_html=True)
        
        # 添加CSS样式
        st.markdown("""
        <style>
        .scrollable-log {
            height: 300px;
            overflow-y: auto;
            border: 1px solid #ddd;
            padding: 10px;
            border-radius: 5px;
            background-color: #f9f9f9;
            margin-bottom: 20px;
            white-space: pre-wrap;
            font-family: monospace;
        }
        </style>
        """, unsafe_allow_html=True)


# 使用session_state来跟踪是否应该显示分析结果
if 'show_analysis' not in st.session_state:
    st.session_state.show_analysis = False

visualize_button = st.button('Visualize CSV')
if visualize_button:
    st.session_state.show_analysis = True

# 如果应该显示分析结果，则显示
if st.session_state.show_analysis:
    contact_delay_analyzer(os.path.join(folder_path, "ctr_data.csv"))

# 搜索功能
if 'contact_id' not in st.session_state:
    st.session_state.contact_id = ''

contact_id = st.text_input('Contact Id', value=st.session_state.contact_id, key='contact_id_input')
st.session_state.contact_id = contact_id

search_button = st.button('Search')
if search_button:
    # 使用搜索函数在 ctr_data.csv 中查找匹配记录
    result_df = search_ctr_data(folder_path, contact_id)
    
    if not result_df.empty:
        st.write(f"找到 {len(result_df)} 条匹配记录:")
        st.dataframe(result_df)
    else:
        st.write("未找到匹配记录。")