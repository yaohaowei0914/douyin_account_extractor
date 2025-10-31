import io
import json
import base64
import csv
import argparse
from datetime import datetime
import os
import glob
import re
from typing import List, Dict, Any, Tuple
import streamlit as st

def har_bytes_to_json(raw: bytes) -> Dict[str, Any] | None:
    """将 HAR 文件字节解析为 JSON 对象。"""
    if not raw:
        return None
    try:
        text = raw.decode("utf-8", errors="ignore")
        return json.loads(text)
    except Exception:
        return None


def load_har_file(file_path):
    """Load and parse HAR file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"Successfully loaded HAR file with {len(data.get('log', {}).get('entries', []))} entries")
            return data
    except json.JSONDecodeError as e:
        print(f"Error parsing HAR file: {e}")
        return None
    except Exception as e:
        print(f"Error reading HAR file: {e}")
        return None

def decode_content(content):
    """Decode content with various encodings."""
    if not content:
        print("Content is empty")
        return None
        
    # If content is already a string, try to parse it as JSON
    if isinstance(content, str):
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            print(f"Failed to parse string as JSON: {str(e)[:100]}")
    
    # Try different encodings for binary data
    encodings = ['utf-8', 'latin1', 'cp1252', 'ascii']
    
    # If it's base64 encoded
    if isinstance(content, str):
        try:
            # Try base64 decoding
            decoded = base64.b64decode(content)
            # Try different encodings for the decoded content
            for encoding in encodings:
                try:
                    text = decoded.decode(encoding)
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse base64 decoded {encoding} text as JSON: {str(e)[:100]}")
                except UnicodeDecodeError as e:
                    print(f"Failed to decode base64 content with {encoding}: {str(e)[:100]}")
        except Exception as e:
            print(f"Failed to decode as base64: {str(e)[:100]}")
            
    # If it's raw binary
    if isinstance(content, (bytes, bytearray)):
        for encoding in encodings:
            try:
                text = content.decode(encoding)
                try:
                    return json.loads(text)
                except json.JSONDecodeError as e:
                    print(f"Failed to parse binary {encoding} text as JSON: {str(e)[:100]}")
            except UnicodeDecodeError as e:
                print(f"Failed to decode binary content with {encoding}: {str(e)[:100]}")
                
    print(f"All decoding attempts failed for content type: {type(content)}")
    if isinstance(content, str):
        print(f"First 100 chars of content: {content[:100]}")
    return None

def extract_simple_fields(obj, prefix=""):
    """Extract only simple fields (non-nested) from an object."""
    simple_fields = {}
    if not isinstance(obj, dict):
        return simple_fields
    
    for key, value in obj.items():
        field_name = f"{prefix}_{key}" if prefix else key
        # Only include simple types (not dict, list, or other complex types)
        if isinstance(value, (str, int, float, bool)) or value is None:
            simple_fields[field_name] = value
        elif isinstance(value, dict) and not value:  # Empty dict
            simple_fields[field_name] = ""
        elif isinstance(value, list):
            # Special handling: join non-empty list for specific keys like search_world
            if key == "search_world":
                try:
                    # Convert each element to string sensibly
                    def to_str(item):
                        if isinstance(item, (str, int, float, bool)) or item is None:
                            return "" if item is None else str(item)
                        if isinstance(item, dict):
                            # Try common fields, fallback to compact JSON
                            for k in ("word", "name", "title"):
                                if k in item and isinstance(item[k], (str, int, float)):
                                    return str(item[k])
                            return json.dumps(item, ensure_ascii=False)
                        return str(item)

                    joined = "、".join([to_str(x) for x in value]) if value else ""
                    simple_fields[field_name] = joined
                except Exception:
                    # Fallback: empty string on error
                    simple_fields[field_name] = ""
            else:
                # Keep behavior: only include empty list as empty string; skip non-empty lists
                if not value:
                    simple_fields[field_name] = ""
    
    return simple_fields

def extract_posts(data):
    posts = []
    
    try:
        if not isinstance(data, dict):
            print(f"Data is not a dictionary, type: {type(data)}")
            return posts
            
        if 'data' not in data:
            print(f"No 'data' key in response. Keys found: {list(data.keys())}")
            return posts
            
        data_section = data['data']
        
        # Look for 'list' in data section
        if 'list' not in data_section:
            print(f"No 'list' key in data section. Keys found: {list(data_section.keys())}")
            return posts
            
        item_list = data_section['list']
        if not isinstance(item_list, list):
            print(f"'list' is not a list, type: {type(item_list)}")
            return posts
            
        print(f"\nProcessing list with {len(item_list)} items")
        
        for i, item in enumerate(item_list):
            if not isinstance(item, dict):
                print(f"Item {i} is not a dictionary, skipping")
                continue
                
            post_data = {}
            
            # Extract aweme_info fields
            if 'aweme_info' in item and isinstance(item['aweme_info'], dict):
                aweme_fields = extract_simple_fields(item['aweme_info'], "aweme")
                post_data.update(aweme_fields)
                print(f"Extracted {len(aweme_fields)} aweme_info fields from item {i}")
            else:
                print(f"Item {i} missing or invalid aweme_info")
                
            # Extract author_info fields  
            if 'author_info' in item and isinstance(item['author_info'], dict):
                author_fields = extract_simple_fields(item['author_info'], "author")
                post_data.update(author_fields)
                print(f"Extracted {len(author_fields)} author_info fields from item {i}")
            else:
                print(f"Item {i} missing or invalid author_info")
                
            if post_data:  # Only add if we extracted some data
                posts.append(post_data)
                print(f"Added item {i} to collection with {len(post_data)} fields")
            else:
                print(f"No data extracted from item {i}")
                        
    except Exception as e:
        print(f"Error extracting posts: {str(e)}")
        
    return posts

def extract_data_from_har(har_data, config: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    """遍历 HAR entries，解码响应，提取帖子并按字段映射输出。"""
    extracted_data: List[Dict[str, Any]] = []
    valid_posts = 0

    if not har_data or 'log' not in har_data or 'entries' not in har_data['log']:
        return extracted_data, valid_posts

    entries = har_data['log']['entries']

    for entry in entries:
        try:
            if 'response' not in entry or 'content' not in entry['response']:
                continue
            content = entry['response']['content']
            if not content.get('text'):
                continue
            mime_type = content.get('mimeType', 'unknown')
            if mime_type == 'image/jpg':
                continue

            data = decode_content(content.get('text', ''))
            if not data:
                continue

            posts = extract_posts(data)
            for post in posts:
                valid_posts += 1
                post_data: Dict[str, Any] = {}
                
                # 提取所有字段
                all_fields = set()
                for p in [post]:
                    all_fields.update(p.keys())
                
                for field in sorted(all_fields):
                    post_data[field] = post.get(field, '')

                # 转换时间戳
                for field_name, value in post_data.items():
                    if 'time' in field_name.lower() and value and str(value).isdigit():
                        try:
                            timestamp = int(value)
                            post_data[field_name] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        except (ValueError, TypeError, OSError):
                            pass

                extracted_data.append(post_data)
        except Exception:
            continue

    return extracted_data, valid_posts


def extract_data(har_data):
    extracted_data = []
    total_entries = 0
    valid_posts = 0
    
    if not har_data or 'log' not in har_data or 'entries' not in har_data['log']:
        print("Invalid HAR data structure")
        print(f"Available keys: {list(har_data.keys()) if har_data else 'None'}")
        return extracted_data
        
    entries = har_data['log']['entries']
    print(f"\nProcessing {len(entries)} entries from HAR file")
    
    for entry in entries:
        total_entries += 1
        try:
            if 'response' not in entry or 'content' not in entry['response']:
                print(f"Skipping entry {total_entries}: No response content")
                continue
                
            content = entry['response']['content']
            if not content.get('text'):
                print(f"Skipping entry {total_entries}: No text content")
                continue
                
            # Skip image/jpg content
            mime_type = content.get('mimeType', 'unknown')
            if mime_type == 'image/jpg':
                print(f"Skipping entry {total_entries}: Image content (image/jpg)")
                continue
                
            print(f"\nProcessing entry {total_entries}:")
            print(f"Content type: {mime_type}")
            
            # Try to decode and parse the response
            data = decode_content(content.get('text', ''))
            if not data:
                print(f"Skipping entry {total_entries}: Could not decode content")
                continue
                
            print(f"Data keys: {list(data.keys())}")
                
            # Extract posts from the response
            posts = extract_posts(data)
            
            for post in posts:
                valid_posts += 1
                extracted_data.append(post)
                print(f"Added post with {len(post)} fields")
                
        except Exception as e:
            print(f"Error processing entry {total_entries}: {str(e)}")
            continue
            
    print(f"\nProcessed {total_entries} total entries")
    print(f"Found {valid_posts} valid posts")
    
    return extracted_data

def to_csv_bytes(rows: List[Dict[str, Any]]) -> bytes:
    """将结果行写成 CSV（二进制，UTF-8-SIG）。"""
    if not rows:
        return b''

    # 获取所有字段名并排序
    all_fields = set()
    for r in rows:
        all_fields.update(r.keys())
    field_order = sorted(list(all_fields))

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=field_order, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode('utf-8-sig')


def save_to_csv(data, output_file):
    """Save extracted data to CSV file."""
    if not data:
        print("No data to save")
        return

    try:
        # Get all unique field names from all posts
        all_fields = set()
        for post in data:
            all_fields.update(post.keys())
        
        # Sort fields for consistent column order
        field_order = sorted(list(all_fields))
        
        # Convert Unix timestamps to Excel-compatible datetime strings for time-related fields
        for post in data:
            for field_name, value in post.items():
                if 'time' in field_name.lower() and value and str(value).isdigit():
                    try:
                        timestamp = int(value)
                        # Convert to Excel-compatible datetime string (YYYY-MM-DD HH:MM:SS)
                        post[field_name] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    except (ValueError, TypeError, OSError):
                        # Keep original value if conversion fails
                        pass

        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=field_order)
            writer.writeheader()
            writer.writerows(data)
        print(f"Successfully saved {len(data)} posts to {output_file}")
        print(f"CSV columns: {field_order}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def process_directory(directory_path, output_file):
    """Process all HAR files in a directory and combine into a single CSV."""
    if not os.path.exists(directory_path):
        print(f"Directory not found: {directory_path}")
        return
        
    har_files = glob.glob(os.path.join(directory_path, "*.har"))
    if not har_files:
        print(f"No HAR files found in directory: {directory_path}")
        return
        
    print(f"Found {len(har_files)} HAR files to process")
    
    all_extracted_data = []
    
    for har_file in har_files:
        print(f"\nProcessing file: {har_file}")
        
        # Load and process the HAR file
        har_data = load_har_file(har_file)
        if not har_data:
            continue
            
        # Extract data from the HAR file
        extracted_data = extract_data(har_data)
        if extracted_data:
            all_extracted_data.extend(extracted_data)
            print(f"Added {len(extracted_data)} posts from {har_file}")
        else:
            print(f"No data extracted from {har_file}")
            
    if all_extracted_data:
        print(f"\nTotal posts collected: {len(all_extracted_data)}")
        save_to_csv(all_extracted_data, output_file)
    else:
        print("No data was extracted from any HAR files")

def main():
    parser = argparse.ArgumentParser(description='Extract Douyin posts from HAR file')
    parser.add_argument(
        '--har',
        help='Path to a HAR file or directory containing HAR files',
        default='./analyse_article/douyin_har_files/'
    )
    parser.add_argument(
        '--output',
        help='Output CSV file path',
        default='./analyse_article/douyin_posts.csv'
    )
    args = parser.parse_args()

    process_directory(args.har, args.output)


# 指定列提取映射（与 extract_douyin_selected_columns.py 保持一致）
SELECTED_HEADERS: List[Tuple[str, str]] = [
    ("author_author_id", "ID"),
    ("author_aweme_count", "视频个数"),
    ("author_comment_avg", "平均评论"),
    ("author_digg_avg", "平均点赞"),
    ("author_follower_count", "粉丝个数"),
    ("author_nickname", "name"),
    ("author_share_avg", "平均分享"),
    ("author_unique_id", "抖音号"),
    ("aweme_aweme_cover", "视频封面"),
    ("aweme_aweme_create_time", "创建时间"),
    ("aweme_aweme_title", "标题"),
    ("aweme_aweme_url", "url"),
    ("aweme_collect_count", "收藏"),
    ("aweme_comment_count", "评论"),
    ("aweme_digg_count", "点赞"),
    ("aweme_share_count", "分享"),
    ("aweme_play_count_v2", "预估播放"),
    ("aweme_sentence", "相关关键检索"),
    ("aweme_search_world", "搜索关键词"),
]

def extract_selected_columns(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """按指定列映射提取并重命名列"""
    selected: List[Dict[str, Any]] = []
    for row in rows:
        new_row = {}
        for src_key, dst_name in SELECTED_HEADERS:
            new_row[dst_name] = row.get(src_key, "")
        selected.append(new_row)
    return selected

def download_selected_csv(rows: List[Dict[str, Any]], filename: str) -> bytes:
    """将指定列结果转为 UTF-8-SIG CSV 字节流"""
    selected = extract_selected_columns(rows)
    if not selected:
        return b""
    buf = io.StringIO()
    # 按 SELECTED_HEADERS 顺序写列
    fieldnames = [dst for _, dst in SELECTED_HEADERS]
    writer = csv.DictWriter(buf, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(selected)
    return buf.getvalue().encode("utf-8-sig")

def streamlit_app():
    st.set_page_config(page_title="抖音 HAR 转 CSV", page_icon="🎵", layout="centered")
    st.title("🎵 抖音 HAR 转 CSV")
    st.markdown(
        "上传一个或多个 `.har` 文件（从浏览器开发者工具的 **Network** 面板导出）。"
        "应用会扫描响应内容，提取抖音帖子字段并合并为一份 CSV。"
    )

    uploaded = st.file_uploader(
        "拖拽或选择 `.har` 文件（可多选）",
        type=["har"],
        accept_multiple_files=True,
        help="Chrome/Edge：打开开发者工具 → Network → 右键空白处 → Save all as HAR with content。"
    )
    st.subheader("📦 指定列导出")

    gen_btn = st.button("生成指定列 CSV")

    if gen_btn:
        if not uploaded:
            st.warning("请至少上传 1 个 `.har` 文件。")
            st.stop()

        all_rows: List[Dict[str, Any]] = []
        progress = st.progress(0)

        # 配置（空配置，因为抖音版本直接提取所有字段）
        config = {'fields': {}}

        for i, uf in enumerate(uploaded, start=1):
            with st.status(f"正在处理 **{uf.name}** …", expanded=False):
                raw = uf.read()
                har_obj = har_bytes_to_json(raw)
                if not har_obj:
                    st.error(f"无法解析 HAR：{uf.name}")
                else:
                    rows, count = extract_data_from_har(har_obj, config)
                    all_rows.extend(rows)
                    st.write(f"在该文件中发现 **{count}** 条帖子。")

            progress.progress(i / len(uploaded))

        if not all_rows:
            st.info("未在所上传的文件中找到可用的帖子数据。")
            st.stop()

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_name = f"douyin_posts_{ts}.csv"

        selected_csv_bytes = download_selected_csv(all_rows, out_name)

        st.success(f"完成！共从 **{len(uploaded)}** 个文件提取 **{len(all_rows)}** 条记录，并生成指定列 CSV。")
        st.download_button(
            "⬇️ 下载指定列 CSV",
            data=selected_csv_bytes,
            file_name=out_name,
            mime="text/csv"
        )
        st.caption("列顺序：ID、视频个数、平均评论、平均点赞、粉丝个数、name、平均分享、抖音号、视频封面、创建时间、标题、url、收藏、评论、点赞、分享、预估播放、相关关键检索、搜索关键词")


if __name__ == '__main__':
    # 检查是否在Streamlit环境中运行
    try:
        # 如果能导入streamlit且在streamlit环境中，运行streamlit应用
        import sys
        if 'streamlit' in sys.modules or any('streamlit' in arg for arg in sys.argv):
            streamlit_app()
        else:
            main()
    except:
        main()
