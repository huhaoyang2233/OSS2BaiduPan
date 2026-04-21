import pymysql
import json
import os
from contextlib import contextmanager

# ==============================
# 数据库配置（从环境变量读取）
# ==============================
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 3306)),
    "user": os.environ.get("DB_USER", "your_username"),
    "password": os.environ.get("DB_PASSWORD", "your_password"),
    "database": os.environ.get("DB_NAME", "qiniu_backup"),
    "charset": "utf8mb4"
}

TABLE_NAME = "file_upload_status"
# ==============================
# 数据库操作函数
# ==============================

def get_pending_files(limit=20):
    """获取pending状态的文件"""
    conn = None
    cursor = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql = f"SELECT file_name FROM {TABLE_NAME} WHERE file_status = 'pending' LIMIT %s"
        cursor.execute(sql, (limit,))
        results = cursor.fetchall()
        return [row[0] for row in results]
    except Exception as e:
        print(f"❌ 读取失败: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def batch_update_status(updates):
    """
    批量更新文件状态
    updates格式: [(file_name, status, error_msg), ...]
    """
    if not updates:
        return True
    
    conn = None
    cursor = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 方案2：使用 executemany（比逐条快，但不如CASE WHEN）
        sql = f"UPDATE {TABLE_NAME} SET file_status = %s, error_message = %s WHERE file_name = %s"
        
        # 准备参数
        params = []
        for file_name, status, error_msg in updates:
            params.append((status, error_msg[:1000] if error_msg else None, file_name))
        
        cursor.executemany(sql, params)
        conn.commit()
        
        print(f"✅ 批量更新 {len(updates)} 条记录")
        return True
        
    except Exception as e:
        print(f"❌ 批量更新失败: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def count_pending_files():
    """统计pending状态的文件数量"""
    conn = None
    cursor = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE file_status = 'pending'"
        cursor.execute(sql)
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        print(f"❌ 统计失败: {e}")
        return 0
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_pending_local_files(file_list):
    """
    从文件列表中找出状态为pending的文件
    file_list: 本地文件路径列表
    返回: pending状态的文件列表
    """
    if not file_list:
        return []
    
    conn = None
    cursor = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 分批查询，避免SQL太长
        batch_size = 100
        pending_files = []
        
        for i in range(0, len(file_list), batch_size):
            batch = file_list[i:i+batch_size]
            placeholders = ','.join(['%s'] * len(batch))
            sql = f"SELECT file_name FROM {TABLE_NAME} WHERE file_name IN ({placeholders}) AND file_status = 'pending'"
            cursor.execute(sql, batch)
            results = cursor.fetchall()
            pending_files.extend([row[0] for row in results])
        
        return pending_files
    except Exception as e:
        print(f"❌ 查询本地pending文件失败: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# 测试代码
if __name__ == "__main__":
    print("测试数据库连接...")
    count = count_pending_files()
    print(f"当前pending文件数: {count}")
    
    files = get_pending_files(5)
    print(f"获取到 {len(files)} 个文件: {files}")