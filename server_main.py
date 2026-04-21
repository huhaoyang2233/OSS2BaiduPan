import os
import json
import requests
import hashlib
import time
import sys
import signal
import pymysql
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from openapi_client.api import fileupload_api
from openapi_client.api import fileinfo_api
import openapi_client
import mysql_helper

class BaiduUploader:
    def __init__(self, access_token, base_path="/jiusong", max_workers=3):
        self.access_token = access_token
        self.base_path = base_path.rstrip('/')
        self.max_workers = max_workers
        self.api_client = openapi_client.ApiClient()
        self.upload_api = fileupload_api.FileuploadApi(self.api_client)
        self.fileinfo_api = fileinfo_api.FileinfoApi(self.api_client)
        self.running = True
        self.download_dir = None
        self.current_batch_updates = []
        self.current_batch_files = []
        self.processing_file = None
        self.interrupted = False
        
        # 线程锁
        self.db_lock = Lock()
        self.stats_lock = Lock()
        
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """处理退出信号"""
        print(f"\n⚠️ 接收到退出信号 {signum}，正在保存当前状态...")
        self.running = False
        self.interrupted = True
        
        if self.processing_file:
            print(f"📝 标记当前文件为失败: {self.processing_file}")
            with self.db_lock:
                self.current_batch_updates.append((self.processing_file, 'failed', '用户中断'))
    
    def safe_exit(self):
        """安全退出，保存当前批次状态"""
        with self.db_lock:
            if self.current_batch_updates and self.interrupted:
                print(f"\n💾 保存当前批次 {len(self.current_batch_updates)} 条状态到数据库...")
                try:
                    mysql_helper.batch_update_status(self.current_batch_updates)
                    print("✅ 状态已保存")
                except Exception as e:
                    print(f"❌ 保存状态失败: {e}")
    
    def check_baidu_file_exists(self, remote_path):
        """检查文件是否已在百度网盘存在"""
        try:
            file_name = os.path.basename(remote_path)
            dir_path = os.path.dirname(remote_path)
            
            url = "https://pan.baidu.com/rest/2.0/xpan/file"
            params = {
                'method': 'search',
                'access_token': self.access_token,
                'key': file_name,
                'dir': dir_path,
                'recursion': 0,
                'page': 1,
                'num': 100
            }
            
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('errno') == 0:
                files = data.get('list', [])
                for file in files:
                    if (file.get('server_filename') == file_name and 
                        file.get('path') == remote_path):
                        return True, file
                return False, None
            else:
                return False, None
                
        except Exception as e:
            return False, None

    def verify_uploaded_files(self):
        """验证本地pending文件是否已上传"""
        print("\n🔍 验证本地pending文件是否已上传...")
        
        all_local = self.get_local_files(self.download_dir)
        if not all_local:
            print("✅ 没有本地文件需要验证")
            return
        
        print(f"📊 扫描到 {len(all_local)} 个本地文件")
        
        success_count = 0
        batch_updates = []
        
        # 多线程验证
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {}
            for file_name in all_local:
                remote_path = f"{self.base_path}/{file_name.lstrip('/')}"
                future = executor.submit(self.check_baidu_file_exists, remote_path)
                future_to_file[future] = file_name
            
            for future in as_completed(future_to_file):
                file_name = future_to_file[future]
                try:
                    exists, _ = future.result()
                    if exists:
                        local_path = os.path.join(self.download_dir, file_name.lstrip('/'))
                        batch_updates.append((file_name, 'success', None))
                        success_count += 1
                        
                        try:
                            os.remove(local_path)
                            dir_path = os.path.dirname(local_path)
                            self.remove_empty_dirs(dir_path)
                        except:
                            pass
                        
                        if len(batch_updates) >= 20:
                            mysql_helper.batch_update_status(batch_updates)
                            batch_updates = []
                except Exception as e:
                    print(f"  ❌ 验证失败: {file_name}")
        
        if batch_updates:
            mysql_helper.batch_update_status(batch_updates)
        
        print(f"\n✅ 验证完成: {success_count} 个本地文件已在百度网盘存在")
    
    def check_local_file(self, local_path, expected_size=None):
        """检查本地文件是否存在且完整"""
        if not os.path.exists(local_path):
            return False, False, None, None
        
        try:
            file_size = os.path.getsize(local_path)
            
            if expected_size is None:
                return True, True, file_size, None
            
            if file_size != expected_size:
                return True, False, file_size, None
            
            if file_size > 100 * 1024 * 1024:
                return True, True, file_size, None
            
            md5 = hashlib.md5()
            with open(local_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    md5.update(chunk)
            file_md5 = md5.hexdigest()
            
            return True, True, file_size, file_md5
            
        except Exception as e:
            return True, False, None, None
    
    def download_file(self, file_info):
        """下载文件到本地"""
        url = file_info['url']
        local_path = file_info['local_path']
        oss_path = file_info['oss_path']
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            head_response = requests.head(url, timeout=30, headers=headers)
            head_response.raise_for_status()
            expected_size = int(head_response.headers.get('content-length', 0))
            
            exists, is_complete, local_size, local_md5 = self.check_local_file(local_path, expected_size)
            
            if exists and is_complete:
                if local_md5 is None:
                    md5 = hashlib.md5()
                    with open(local_path, 'rb') as f:
                        for chunk in iter(lambda: f.read(8192), b''):
                            md5.update(chunk)
                    local_md5 = md5.hexdigest()
                
                return {
                    'oss_path': oss_path,
                    'success': True,
                    'local_path': local_path,
                    'remote_path': file_info['remote_path'],
                    'file_size': local_size,
                    'file_md5': local_md5,
                    'error': None,
                    'from_cache': True
                }
            elif exists and not is_complete:
                try:
                    os.remove(local_path)
                except:
                    pass
                
            print(f"⬇️ 下载: {oss_path}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            headers = {'User-Agent': 'Mozilla/5.0'}
            if exists and not is_complete:
                headers['Range'] = f'bytes={local_size}-'
            
            session = requests.Session()
            
            try:
                response = session.get(url, headers=headers, stream=True, timeout=(10, 30))
                response.raise_for_status()
                
                mode = 'ab' if (exists and not is_complete) else 'wb'
                
                with open(local_path, mode) as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if not self.running:
                            raise Exception("下载被用户中断")
                        if chunk:
                            f.write(chunk)
                
                final_size = os.path.getsize(local_path)
                if expected_size and final_size != expected_size:
                    return {
                        'oss_path': oss_path,
                        'success': False,
                        'error': f'文件大小不匹配: 预期{expected_size}, 实际{final_size}',
                        'local_path': None
                    }
                
                md5 = hashlib.md5()
                with open(local_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(8192), b''):
                        md5.update(chunk)
                file_md5 = md5.hexdigest()
                print(f"  ✅ 下载完成: {final_size}/{expected_size} bytes")
                return {
                    'oss_path': oss_path,
                    'success': True,
                    'local_path': local_path,
                    'remote_path': file_info['remote_path'],
                    'file_size': final_size,
                    'file_md5': file_md5,
                    'error': None,
                    'from_cache': False
                }
                
            finally:
                session.close()
                
        except Exception as e:
            return {
                'oss_path': oss_path,
                'success': False,
                'error': str(e),
                'local_path': None
            }
    
    def upload_file(self, file_info):
        """上传文件到百度云盘"""
        local_path = file_info['local_path']
        remote_path = file_info['remote_path']
        oss_path = file_info['oss_path']
        file_size = file_info['file_size']
        file_md5 = file_info['file_md5']
        
        if not os.path.exists(local_path):
            return {
                'oss_path': oss_path,
                'success': False,
                'error': '本地文件不存在',
                'should_delete': False,
                'local_path': local_path  # 添加这行
            }
        
        block_list = json.dumps([file_md5])
        if not remote_path.startswith('/'):
            remote_path = '/' + remote_path
        
        try:
            print(f"  ⏳ 上传: {oss_path}")
            start_time = time.time()
            precreate_response = self.upload_api.xpanfileprecreate(
                access_token=self.access_token,
                path=remote_path,
                isdir=0,
                size=file_size,
                autoinit=1,
                block_list=block_list,
                rtype=3
            )
            
            if isinstance(precreate_response, dict):
                errno = precreate_response.get('errno')
                if errno != 0:
                    return {
                        'oss_path': oss_path, 
                        'success': False, 
                        'error': f'预创建失败 ({errno})',
                        'should_delete': False,
                        'local_path': local_path  # 添加这行
                    }
                uploadid = precreate_response.get('uploadid')
            else:
                if precreate_response.errno != 0:
                    return {
                        'oss_path': oss_path, 
                        'success': False, 
                        'error': f'预创建失败 ({precreate_response.errno})',
                        'should_delete': False,
                        'local_path': local_path  # 添加这行
                    }
                uploadid = precreate_response.uploadid
            
            with open(local_path, 'rb') as f:
                upload_response = self.upload_api.pcssuperfile2(
                    access_token=self.access_token,
                    partseq="0",
                    path=remote_path,
                    uploadid=uploadid,
                    type="tmpfile",
                    file=f
                )
            
            if isinstance(upload_response, dict):
                errno = upload_response.get('errno')
                if errno is not None and errno != 0:
                    return {
                        'oss_path': oss_path, 
                        'success': False, 
                        'error': f'上传失败 ({errno})',
                        'should_delete': False,
                        'local_path': local_path  # 添加这行
                    }
            else:
                if hasattr(upload_response, 'errno') and upload_response.errno != 0:
                    return {
                        'oss_path': oss_path, 
                        'success': False, 
                        'error': f'上传失败 ({upload_response.errno})',
                        'should_delete': False,
                        'local_path': local_path  # 添加这行
                    }
            
            create_response = self.upload_api.xpanfilecreate(
                access_token=self.access_token,
                path=remote_path,
                isdir=0,
                size=file_size,
                uploadid=uploadid,
                block_list=block_list,
                rtype=3
            )
            
            if isinstance(create_response, dict):
                errno = create_response.get('errno')
                if errno == 0:
                    elapsed = time.time() - start_time
                    print(f"  ✅ 上传成功: {oss_path} ({elapsed:.1f}秒)")
                    return {
                        'oss_path': oss_path, 
                        'success': True, 
                        'error': None,
                        'should_delete': True,
                        'local_path': local_path  # 确保包含 local_path
                    }
                else:
                    return {
                        'oss_path': oss_path, 
                        'success': False, 
                        'error': f'创建失败 ({errno})',
                        'should_delete': False,
                        'local_path': local_path  # 添加这行
                    }
            else:
                if create_response.errno == 0:
                    elapsed = time.time() - start_time
                    print(f"  ✅ 上传成功: {oss_path} ({elapsed:.1f}秒)")
                    return {
                        'oss_path': oss_path, 
                        'success': True, 
                        'error': None,
                        'should_delete': True,
                        'local_path': local_path  # 确保包含 local_path
                    }
                else:
                    return {
                        'oss_path': oss_path, 
                        'success': False, 
                        'error': f'创建失败 ({create_response.errno})',
                        'should_delete': False,
                        'local_path': local_path  # 添加这行
                    }
                    
        except Exception as e:
            return {
                'oss_path': oss_path, 
                'success': False, 
                'error': str(e),
                'should_delete': False,
                'local_path': local_path  # 添加这行
            }
    
    def safe_delete_file(self, file_path, oss_path):
        """安全删除文件"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                dir_path = os.path.dirname(file_path)
                self.remove_empty_dirs(dir_path)
                return True
        except Exception as e:
            pass
        return False
    
    def remove_empty_dirs(self, path):
        """递归删除空目录"""
        while path and path != '.' and path != '\\' and path != '/' and path != self.download_dir:
            try:
                if os.path.exists(path) and not os.listdir(path):
                    os.rmdir(path)
                    path = os.path.dirname(path)
                else:
                    break
            except:
                break
    
    def print_progress_bar(self, current, total, success, failed, bar_length=40):
        """打印进度条"""
        percent = current / total if total > 0 else 0
        arrow = '█' * int(round(percent * bar_length))
        spaces = ' ' * (bar_length - len(arrow))
        sys.stdout.write(f'\r[{arrow}{spaces}] {current}/{total} | ✅ {success} | ❌ {failed}')
        sys.stdout.flush()
    
    def get_local_files(self, download_dir):
        """获取下载目录中的所有文件（相对路径）"""
        local_files = []
        if os.path.exists(download_dir):
            for root, dirs, files in os.walk(download_dir):
                for file in files:
                    rel_path = os.path.relpath(os.path.join(root, file), download_dir)
                    rel_path = rel_path.replace('\\', '/')
                    local_files.append(rel_path)
        return local_files
    
    def cleanup_success_files(self, download_dir):
        """清理本地残留的已成功文件"""
        all_local = self.get_local_files(download_dir)
        if not all_local:
            return
        
        batch_size = 100
        deleted_count = 0
        
        for i in range(0, len(all_local), batch_size):
            batch = all_local[i:i+batch_size]
            
            try:
                conn = pymysql.connect(**mysql_helper.DB_CONFIG)
                cursor = conn.cursor()
                
                placeholders = ','.join(['%s'] * len(batch))
                sql = f"SELECT file_name FROM {mysql_helper.TABLE_NAME} WHERE file_name IN ({placeholders}) AND file_status = 'success'"
                cursor.execute(sql, batch)
                results = cursor.fetchall()
                success_files = [row[0] for row in results]
                
                for file_name in success_files:
                    clean_path = file_name.lstrip('/')
                    local_path = os.path.join(download_dir, clean_path)
                    if os.path.exists(local_path):
                        try:
                            os.remove(local_path)
                            deleted_count += 1
                            dir_path = os.path.dirname(local_path)
                            self.remove_empty_dirs(dir_path)
                        except:
                            pass
                
            except Exception as e:
                print(f"  ❌ 查询失败: {e}")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
    
    def process_file(self, file_info):
        """处理单个文件（下载+上传）"""
        oss_path = file_info['oss_path']
        local_path = file_info['local_path']
        
        try:
            # 检查本地文件
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                # 直接上传
                file_size = os.path.getsize(local_path)
                md5 = hashlib.md5()
                with open(local_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(8192), b''):
                        md5.update(chunk)
                file_md5 = md5.hexdigest()
                
                file_info.update({
                    'file_size': file_size,
                    'file_md5': file_md5
                })
                
                result = self.upload_file(file_info)
                # 确保结果中包含 local_path 用于删除
                if result.get('success'):
                    result['local_path'] = local_path
                return result
            else:
                # 需要下载
                download_result = self.download_file(file_info)
                if download_result.get('success'):
                    result = self.upload_file(download_result)
                    # 确保结果中包含 local_path 用于删除
                    if result.get('success'):
                        result['local_path'] = download_result['local_path']
                    return result
                else:
                    return download_result
                
        except Exception as e:
            return {
                'oss_path': oss_path,
                'success': False,
                'error': str(e),
                'should_delete': False,
                'local_path': None
            }
    
    def process_with_local_priority(self, 
                                base_url="http://qiniu.shizhenjiankang.com", 
                                download_dir="./downloads", 
                                batch_size=20):
        """
        优先处理本地文件，然后再从数据库补全
        每批处理结束后立即更新数据库，并支持中断安全退出
        多线程版本
        """
        self.download_dir = download_dir
        
        try:
            # ===== 第一步：启动时只检查一次本地文件是否已上传 =====
            print("\n🔍 启动时检查本地文件是否已在百度网盘存在...")
            self.verify_uploaded_files()
            
            # ===== 第二步：清理已成功的本地残留文件 =====
            print("\n🧹 清理已成功但残留的本地文件...")
            self.cleanup_success_files(download_dir)
            
            # 统计数据库中待处理文件总数
            total_db_pending = mysql_helper.count_pending_files()
            print(f"\n📊 数据库中pending文件数: {total_db_pending}")
            
            # 获取所有本地文件
            all_local = self.get_local_files(download_dir)
            print(f"🔍 扫描到 {len(all_local)} 个本地文件")
            
            # 查询哪些本地文件是pending状态
            pending_local = all_local
            print(f"📦 本地pending文件: {len(pending_local)} 个")
            
            if len(pending_local) == 0 and total_db_pending == 0:
                print("✅ 没有需要处理的文件")
                return
            
            total_files = len(pending_local) + total_db_pending
            print(f"📊 待处理总数: {total_files} | 批次大小: {batch_size} | 线程数: {self.max_workers}")
            print("=" * 70)
            
            # 处理队列
            process_queue = pending_local.copy()
            processed_count = 0
            total_success = 0
            total_failed = 0
            batch_num = 1
            
            while self.running and processed_count < total_files:
                batch_start_time = time.time()
                
                # 构建当前批次
                self.current_batch_files = []
                files_to_process = []
                
                # 1. 先从队列取（优先本地pending）
                while len(self.current_batch_files) < batch_size and process_queue:
                    self.current_batch_files.append(process_queue.pop(0))
                
                # 2. 如果队列空了，从数据库取
                if len(self.current_batch_files) < batch_size:
                    need_count = batch_size - len(self.current_batch_files)
                    db_files = mysql_helper.get_pending_files(limit=need_count)
                    
                    for f in db_files:
                        if f not in self.current_batch_files:
                            self.current_batch_files.append(f)
                
                if not self.current_batch_files:
                    break
                
                print(f"\n📦 批次 {batch_num}: 处理 {len(self.current_batch_files)} 个文件")
                
                # 准备文件信息
                for oss_path in self.current_batch_files:
                    clean_path = oss_path.lstrip('/')
                    local_path = os.path.join(download_dir, clean_path)
                    file_url = f"{base_url.rstrip('/')}/{clean_path}"
                    remote_path = f"{self.base_path}/{clean_path}"
                    
                    files_to_process.append({
                        'oss_path': oss_path,
                        'url': file_url,
                        'local_path': local_path,
                        'remote_path': remote_path
                    })
                
                # 重置当前批次的状态更新
                self.current_batch_updates = []
                batch_success = 0
                batch_failed = 0
                files_to_delete = []
                
                # 多线程处理当前批次
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # 提交所有任务
                    future_to_file = {
                        executor.submit(self.process_file, file_info): file_info['oss_path']
                        for file_info in files_to_process
                    }
                    
                    # 收集结果
                    for future in as_completed(future_to_file):
                        oss_path = future_to_file[future]
                        self.processing_file = oss_path
                        
                        try:
                            result = future.result()
                            
                            if result.get('success'):
                                with self.stats_lock:
                                    batch_success += 1
                                self.current_batch_updates.append((oss_path, 'success', None))
                                if result.get('should_delete') and result.get('local_path'):
                                    files_to_delete.append((oss_path, result['local_path']))
                            else:
                                with self.stats_lock:
                                    batch_failed += 1
                                self.current_batch_updates.append((oss_path, 'failed', result.get('error')))
                                
                        except Exception as e:
                            with self.stats_lock:
                                batch_failed += 1
                            self.current_batch_updates.append((oss_path, 'failed', str(e)))
                        
                        self.processing_file = None
                        
                        # 实时更新进度
                        with self.stats_lock:
                            current_processed = processed_count + batch_success + batch_failed
                            current_success = total_success + batch_success
                            current_failed = total_failed + batch_failed
                        
                        self.print_progress_bar(current_processed, total_files, current_success, current_failed)
                
                # 更新数据库
                if self.current_batch_updates:
                    print(f"\n📝 更新本批 {len(self.current_batch_updates)} 条状态到数据库...")
                    mysql_helper.batch_update_status(self.current_batch_updates)
                
                # 删除上传成功的文件
                for oss_path, local_path in files_to_delete:
                    self.safe_delete_file(local_path, oss_path)
                print("🧹清除",len(files_to_delete),"个本地文件")
                total_success += batch_success
                total_failed += batch_failed
                processed_count += len(self.current_batch_files)
                
                batch_time = time.time() - batch_start_time
                print(f" | 批次{batch_num}: {batch_time:.1f}s | 本批: ✅{batch_success} ❌{batch_failed}")
                
                batch_num += 1
                
                if self.running:
                    time.sleep(2)
            
            if not self.running:
                print(f"\n⏸️ 程序暂停，已处理 {processed_count} 个文件")
                print(f"✅ 成功: {total_success}, ❌ 失败: {total_failed}")
                
                self.safe_exit()
            else:
                print(f"\n{'=' * 70}")
                print(f"🎉 完成! 总处理: {processed_count} | 成功: {total_success} | 失败: {total_failed}")
                
        except Exception as e:
            print(f"\n❌ 程序异常: {e}")
            self.safe_exit()
            raise
        finally:
            if self.interrupted:
                print("\n👋 程序已安全退出，所有当前批次状态已保存")


if __name__ == "__main__":
    import os
    
    ACCESS_TOKEN = os.environ.get("BAIDU_ACCESS_TOKEN", "your_access_token")
    
    uploader = BaiduUploader(ACCESS_TOKEN, base_path="/backup", max_workers=30)
    
    uploader.process_with_local_priority(
        base_url=os.environ.get("OSS_BASE_URL", "http://your-oss-domain.com"),
        download_dir="./downloads",
        batch_size=100
    )