# OSS 数据迁移到百度网盘工具

> 一个高效、可靠的工具，用于将七牛云 OSS（或其他支持 HTTP 访问的存储服务）中的数据迁移到百度网盘

## 项目背景

在企业数据迁移场景中，我们常常需要将数据从一个云存储服务迁移到另一个。然而，百度网盘作为国内主流的云存储服务，**并不支持标准的对象存储协议（如 S3、OSS）**，无法直接通过协议进行数据迁移。

本项目解决了这一痛点，通过 HTTP 下载 + 百度网盘开放 API 上传的方式，实现了从任意支持 HTTP 访问的存储服务到百度网盘的数据迁移。

## 核心特性

- **多线程并行处理**: 支持多线程同时下载和上传，提高迁移效率
- **断点续传**: 支持本地文件缓存，中断后可恢复
- **状态持久化**: 使用 MySQL 记录每个文件的迁移状态
- **安全退出**: 支持优雅中断，保证数据一致性
- **自动重试**: 失败文件自动标记，支持后续重新处理

## 技术架构

```
┌─────────────────┐     HTTP GET     ┌─────────────────┐
│   七牛云 OSS     │ ────────────────►│   本地临时存储   │
│ (或其他HTTP存储) │◄─────────────────│   (./downloads) │
└─────────────────┘     缓存检查      └────────┬────────┘
                                               │
                                               │ 百度网盘开放API
                                               ▼
┌─────────────────┐     SQL查询/更新    ┌─────────────────┐
│   MySQL 数据库   │ ◄──────────────────│   百度网盘      │
│ (状态管理)       │───────────────────►│ (目标存储)      │
└─────────────────┘     状态同步       └─────────────────┘
```

## 核心实现

### 1. 文件下载模块

```python
def download_file(self, file_info):
    url = file_info['url']
    local_path = file_info['local_path']
    
    # 检查本地缓存
    exists, is_complete, local_size, local_md5 = self.check_local_file(local_path, expected_size)
    
    if exists and is_complete:
        return {'success': True, 'from_cache': True, ...}
    
    # 支持断点续传
    if exists and not is_complete:
        headers['Range'] = f'bytes={local_size}-'
    
    # 流式下载
    with open(local_path, mode) as f:
        for chunk in response.iter_content(chunk_size=8192):
            if not self.running:
                raise Exception("下载被用户中断")
            if chunk:
                f.write(chunk)
```

**关键点**:
- 下载前先检查本地是否已存在完整文件（通过 MD5 校验）
- 支持断点续传（使用 HTTP Range 请求头）
- 实时检测中断信号，支持安全退出

### 2. 文件上传模块（百度网盘 API）

百度网盘开放 API 的上传流程分为三个步骤：

```python
def upload_file(self, file_info):
    # Step 1: 预创建文件（获取 uploadid）
    precreate_response = self.upload_api.xpanfileprecreate(
        access_token=self.access_token,
        path=remote_path,
        isdir=0,
        size=file_size,
        autoinit=1,
        block_list=json.dumps([file_md5]),
        rtype=3
    )
    
    # Step 2: 上传文件分片
    upload_response = self.upload_api.pcssuperfile2(
        access_token=self.access_token,
        partseq="0",
        path=remote_path,
        uploadid=uploadid,
        type="tmpfile",
        file=f
    )
    
    # Step 3: 完成文件创建
    create_response = self.upload_api.xpanfilecreate(
        access_token=self.access_token,
        path=remote_path,
        uploadid=uploadid,
        block_list=json.dumps([file_md5])
    )
```

**关键点**:
- 使用百度网盘开放 API 的分片上传机制
- 支持大文件（>100MB）自动跳过 MD5 校验
- 完整的错误处理和状态返回

### 3. 状态管理模块

使用 MySQL 数据库持久化文件迁移状态：

| 字段 | 类型 | 说明 |
|------|------|------|
| file_name | VARCHAR | 文件路径（主键） |
| file_status | VARCHAR | pending/success/failed |
| error_message | TEXT | 失败原因 |
| created_at | DATETIME | 创建时间 |
| updated_at | DATETIME | 更新时间 |

```python
def batch_update_status(updates):
    """批量更新文件状态"""
    sql = "UPDATE file_upload_status SET file_status = %s, error_message = %s WHERE file_name = %s"
    params = []
    for file_name, status, error_msg in updates:
        params.append((status, error_msg[:1000] if error_msg else None, file_name))
    cursor.executemany(sql, params)
```

### 4. 多线程并行处理

```python
def process_with_local_priority(self, ...):
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        # 提交所有任务
        future_to_file = {
            executor.submit(self.process_file, file_info): file_info['oss_path']
            for file_info in files_to_process
        }
        
        # 收集结果
        for future in as_completed(future_to_file):
            result = future.result()
            if result.get('success'):
                self.current_batch_updates.append((oss_path, 'success', None))
            else:
                self.current_batch_updates.append((oss_path, 'failed', result.get('error')))
```

**处理流程**:
1. 优先处理本地已缓存的文件
2. 从数据库获取待处理文件列表
3. 按批次并行处理（每批 100 个文件）
4. 批次处理完成后立即更新数据库状态
5. 删除上传成功的本地文件

### 5. 安全退出机制

```python
def signal_handler(self, signum, frame):
    """处理退出信号"""
    self.running = False
    self.interrupted = True
    
    if self.processing_file:
        with self.db_lock:
            self.current_batch_updates.append((self.processing_file, 'failed', '用户中断'))

def safe_exit(self):
    """安全退出，保存当前批次状态"""
    if self.current_batch_updates and self.interrupted:
        mysql_helper.batch_update_status(self.current_batch_updates)
```

## 快速开始

### 环境要求

- Python 3.8+
- MySQL 5.7+
- 百度网盘开放平台账号

### 安装依赖

```bash
pip install -r requirements.txt
```

### 配置说明

**推荐使用环境变量进行配置，避免硬编码敏感信息**

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `DB_HOST` | 数据库主机地址 | localhost |
| `DB_PORT` | 数据库端口 | 3306 |
| `DB_USER` | 数据库用户名 | your_username |
| `DB_PASSWORD` | 数据库密码 | your_password |
| `DB_NAME` | 数据库名称 | qiniu_backup |
| `BAIDU_ACCESS_TOKEN` | 百度网盘 Access Token | - |
| `OSS_BASE_URL` | OSS 文件访问域名 | - |

**设置环境变量示例**:

```bash
# Linux/Mac
export DB_HOST="your-db-host"
export DB_USER="your-db-user"
export DB_PASSWORD="your-db-password"
export BAIDU_ACCESS_TOKEN="your-baidu-access-token"
export OSS_BASE_URL="http://your-oss-domain.com"

# Windows (PowerShell)
$env:DB_HOST="your-db-host"
$env:DB_USER="your-db-user"
$env:DB_PASSWORD="your-db-password"
$env:BAIDU_ACCESS_TOKEN="your-baidu-access-token"
$env:OSS_BASE_URL="http://your-oss-domain.com"
```

**数据库表结构**:

```sql
CREATE TABLE file_upload_status (
    file_name VARCHAR(1024) PRIMARY KEY,
    file_status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

> **获取 Access Token**: 访问 [百度网盘开放平台](https://pan.baidu.com/union/document/entrance) 创建应用并获取

### 使用方法

```bash
# 设置环境变量后直接运行
python server_main.py
```

**或使用 Python 脚本**:

```python
import os
from server_main import BaiduUploader

# 从环境变量获取配置
access_token = os.environ.get("BAIDU_ACCESS_TOKEN")
base_url = os.environ.get("OSS_BASE_URL", "http://your-oss-domain.com")

# 初始化上传器
uploader = BaiduUploader(
    access_token=access_token,
    base_path="/backup",  # 百度网盘目标目录
    max_workers=30       # 并发线程数
)

# 开始迁移
uploader.process_with_local_priority(
    base_url=base_url,
    download_dir="./downloads",
    batch_size=100
)
```

## 注意事项

### 1. 百度网盘 API 限制

- **单个文件大小限制**: 普通用户最大 4GB，超级会员最大 20GB
- **API 调用频率限制**: 建议设置合理的线程数（10-30）
- **Access Token 有效期**: 默认 30 天，需要定期刷新

### 2. 数据完整性保障

- 使用 MD5 校验确保文件完整性
- 大文件（>100MB）跳过 MD5 校验以提高性能
- 上传前检查目标路径是否已存在同名文件

### 3. 网络稳定性

- 设置合理的超时时间
- 支持断点续传，网络中断后可恢复
- 建议在网络稳定的环境下运行大规模迁移

### 4. 资源占用

- 本地临时目录需要足够的磁盘空间
- 多线程模式会增加内存占用
- 建议定期清理已成功上传的本地文件

## 常见问题

### Q: Access Token 如何获取？

A: 在 [百度网盘开放平台](https://pan.baidu.com/union/document/entrance) 创建应用，使用 OAuth 2.0 授权获取。

### Q: 如何处理上传失败的文件？

A: 失败的文件会在数据库中标记为 `failed` 状态，并记录错误信息。可以重新运行程序，会自动处理这些失败的文件。

### Q: 支持哪些源存储服务？

A: 支持任何提供 HTTP 直接访问的存储服务，包括：
- 七牛云 OSS
- 阿里云 OSS（需开启外网访问）
- AWS S3（需配置静态网站托管）
- 其他支持 HTTP 访问的存储服务

### Q: 如何提高迁移速度？

A: 
1. 增加 `max_workers` 参数（建议 20-30）
2. 使用更快的网络连接
3. 确保本地磁盘写入速度足够快
4. 避免在高峰时段进行迁移

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

---

**项目地址**: [GitHub Repository](https://github.com/yourusername/qiniu_to_baidu_mysql_mult_template)