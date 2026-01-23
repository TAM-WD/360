import boto3
import hashlib
import base64
from datetime import datetime
import os
import requests
import time
from pathlib import Path
import shutil
import csv
import gc

# S3 конфигурация
AWS_KEY_ID="" # ID ключа AWS
AWS_SECRET_KEY="" # секретный ключ AWS
BUCKET_NAME = "" # имя бакета
REGION = "ru-central1" # регион бакета

# Яндекс Диск конфигурация (OAuth токен организации)
YANDEX_ORG_TOKEN = "" # токен с правами на доступ к Диску
VD_HASH = ""  # VD_hash общего диска, получить можно через API (обязательно!)
VD_NAME = ""  # Имя для логов и структуры в S3
S3_BASE_FOLDER = ""  # Базовая папка в S3

# Локальные настройки
TEMP_DOWNLOAD_DIR = None
MULTIPART_THRESHOLD = 100 * 1024 * 1024
MULTIPART_CHUNK_SIZE = 10 * 1024 * 1024
MAX_LOG_SIZE = 10 * 1024 * 1024
LOG_DIR = None
LOG_FILE = None
CSV_FILE = None
LOG_PART = 1

# Константы API
VIRTUAL_DISKS_API_BASE = "https://cloud-api.yandex.net/v1/disk/virtual-disks"


def init_logging():
    """Инициализирует систему логирования"""
    global LOG_DIR, LOG_FILE, CSV_FILE, LOG_PART, TEMP_DOWNLOAD_DIR
    
    script_dir = Path(__file__).parent.resolve()
    script_name = Path(__file__).stem
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    LOG_DIR = script_dir / f"{script_name}_{timestamp}"
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    TEMP_DOWNLOAD_DIR = LOG_DIR / "temp_downloads"
    TEMP_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    LOG_FILE = str(LOG_DIR / f"sync_{timestamp}.log")
    LOG_PART = 1
    
    CSV_FILE = str(LOG_DIR / f"sync_{timestamp}.csv")
    init_csv()
    
    return LOG_FILE


def init_csv():
    """Инициализирует CSV файл с заголовками"""
    with open(CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Timestamp',
            'VD Name',
            'VD Hash',
            'Type',
            'Name',
            'VD Path',
            'S3 Path',
            'Size (bytes)',
            'Size (MB)',
            'Status',
            'Upload Method',
            'MD5',
            'Duration (sec)',
            'Error Message'
        ])


def write_csv_row(row_data):
    """Добавляет строку в CSV файл"""
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(row_data)
    except Exception as e:
        log_message(f"WARNING: Cannot write to CSV: {e}")


def check_log_rotation():
    """Проверяет размер лог-файла и создаёт новый при превышении лимита"""
    global LOG_FILE, LOG_PART
    
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > MAX_LOG_SIZE:
        LOG_PART += 1
        log_path = Path(LOG_FILE)
        base_name = log_path.stem.rsplit('_part', 1)[0]
        LOG_FILE = str(log_path.parent / f"{base_name}_part{LOG_PART}.log")
        log_message(f">>> Log file rotated to part {LOG_PART} <<<", skip_rotation_check=True)


def log_message(message, skip_rotation_check=False):
    """Записывает сообщение в лог и выводит в консоль"""
    timestamp_msg = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {message}"
    print(timestamp_msg)
    
    if not skip_rotation_check:
        check_log_rotation()
    
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(timestamp_msg + '\n')


def calculate_md5(file_path):
    """Вычисляет MD5 хеш файла"""
    hash_md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5


def normalize_s3_path(*parts):
    """Нормализует и объединяет части пути для S3"""
    filtered_parts = [p.strip('/') for p in parts if p and p.strip('/')]
    if not filtered_parts:
        return ""
    return '/'.join(filtered_parts)


def build_vd_path(vd_hash, inner_path):
    """
    Формирует путь в формате vd:<vd_hash>:disk:/<путь>
    
    vd_hash: метка общего диска
    inner_path: путь внутри диска (например, "/" или "/Папка1/файл.txt")
    """
    if not inner_path.startswith('/'):
        inner_path = '/' + inner_path
    
    return f"vd:{vd_hash}:disk:{inner_path}"


def normalize_vd_path(api_path):
    """
    Нормализует путь из формата API в правильный формат
    """
    if not api_path.startswith('vd:'):
        return api_path
    
    path_without_prefix = api_path[3:]
    
    if path_without_prefix.startswith('/'):
        parts = path_without_prefix.lstrip('/').split('/', 2)
        if len(parts) >= 2 and parts[1] == 'disk':
            vd_hash = parts[0]
            inner_path = '/' + parts[2] if len(parts) > 2 else '/'
            return f"vd:{vd_hash}:disk:{inner_path}"
    elif ':disk:' in path_without_prefix:
        return api_path
    
    return api_path


def extract_inner_path(vd_full_path):
    """
    Извлекает внутренний путь из полного пути vd
    """
    if ':disk:' in vd_full_path:
        return vd_full_path.split(':disk:', 1)[1]
    elif vd_full_path.startswith('vd:/'):
        parts = vd_full_path[4:].split('/', 2)
        if len(parts) >= 2 and parts[1] == 'disk':
            return '/' + parts[2] if len(parts) > 2 else '/'
    
    return vd_full_path


def get_vd_metadata(vd_hash, inner_path, token, limit=1000, offset=0):
    """
    Получает метаданные файла или папки на виртуальном диске
    
    vd_hash: метка общего диска
    inner_path: путь внутри диска (например, "/" или "/Папка1")
    """
    url = f"{VIRTUAL_DISKS_API_BASE}/resources"
    
    full_path = build_vd_path(vd_hash, inner_path)
    
    params = {
        'path': full_path,
        'limit': limit,
        'offset': offset,
        'fields': '_embedded.items.name,_embedded.items.path,_embedded.items.type,_embedded.items.size,_embedded.items.created,_embedded.items.modified,_embedded.items.media_type,_embedded.total,name,path,type'
    }
    
    headers = {
        'Authorization': f'OAuth {token}',
        'Accept': 'application/json'
    }
    
    try:
        log_message(f"Fetching metadata for: {full_path}")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        log_message(f"ERROR: Failed to get metadata for {full_path}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            log_message(f"Response status: {e.response.status_code}")
            log_message(f"Response body: {e.response.text}")
        return None


def get_vd_download_link(vd_path_or_inner, token, vd_hash=None):
    """
    Получает ссылку для скачивания файла с виртуального диска
    https://yandex.ru/dev/disk-api/doc/ru/reference/content_shd
    
    vd_path_or_inner: либо полный путь vd:hash:disk:/path, либо внутренний путь /path
    token: OAuth токен
    vd_hash: если передан внутренний путь, нужен vd_hash для формирования полного пути
    """
    url = f"{VIRTUAL_DISKS_API_BASE}/resources/download"
    
    if vd_path_or_inner.startswith('vd:'):
        full_path = vd_path_or_inner
    else:
        if not vd_hash:
            log_message("ERROR: vd_hash required for inner path")
            return None
        full_path = build_vd_path(vd_hash, vd_path_or_inner)
    
    params = {
        'path': full_path
    }
    
    headers = {
        'Authorization': f'OAuth {token}',
        'Accept': 'application/json'
    }
    
    try:
        log_message(f"Requesting download link...")
        log_message(f"  API URL: {url}")
        log_message(f"  Full path param: {full_path}")
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        log_message(f"  Response status: {response.status_code}")
        
        if response.status_code != 200:
            log_message(f"  ERROR Response body: {response.text}")
            return None
            
        response.raise_for_status()
        
        data = response.json()
        
        href = data.get('href')
        if href:
            log_message(f"  ✓ Download link obtained: {href[:80]}...")
            return href
        else:
            log_message(f"  ERROR: No 'href' in response")
            log_message(f"  Full response: {data}")
            return None
            
    except requests.exceptions.RequestException as e:
        log_message(f"ERROR: Failed to get download link: {e}")
        if hasattr(e, 'response') and e.response is not None:
            log_message(f"  Response status: {e.response.status_code}")
            log_message(f"  Response body: {e.response.text}")
        return None


def download_file(download_url, local_path):
    """Скачивает файл по ссылке"""
    try:
        log_message(f"Downloading to {local_path}...")
        
        local_path_obj = Path(local_path)
        local_path_obj.parent.mkdir(parents=True, exist_ok=True)
        
        response = requests.get(download_url, stream=True, timeout=60, allow_redirects=True)
        
        log_message(f"  Download response status: {response.status_code}")
        
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        last_log_mb = 0
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    current_mb = downloaded // (1024 * 1024)
                    if total_size > 0 and current_mb > last_log_mb and current_mb % 10 == 0:
                        progress = (downloaded / total_size) * 100
                        log_message(f"  Progress: {progress:.1f}% ({current_mb} MB / {total_size // (1024*1024)} MB)")
                        last_log_mb = current_mb
        
        log_message(f"  ✓ Downloaded: {downloaded} bytes ({downloaded/1024/1024:.2f} MB)")
        
        response.close()
        del response
        
        return True
        
    except Exception as e:
        log_message(f"ERROR: Failed to download file: {e}")
        import traceback
        log_message(f"Traceback: {traceback.format_exc()}")
        return False


def upload_large_file_multipart(local_file_path, s3_object_path, s3_client):
    """Загружает большой файл в S3 используя multipart upload"""
    try:
        file_size = os.path.getsize(local_file_path)
        log_message(f"Starting MULTIPART upload ({file_size/1024/1024:.2f} MB)")
        
        log_message("Calculating MD5...")
        full_md5 = calculate_md5(local_file_path).hexdigest()
        log_message(f"MD5: {full_md5}")
        
        response = s3_client.create_multipart_upload(
            Bucket=BUCKET_NAME,
            Key=s3_object_path
        )
        upload_id = response['UploadId']
        log_message(f"Upload ID: {upload_id}")
        
        parts = []
        part_number = 1
        uploaded_bytes = 0
        
        try:
            with open(local_file_path, 'rb') as f:
                while True:
                    chunk = f.read(MULTIPART_CHUNK_SIZE)
                    if not chunk:
                        break
                    
                    chunk_size = len(chunk)
                    log_message(f"Uploading part {part_number} ({chunk_size/1024/1024:.2f} MB)...")
                    
                    part_response = s3_client.upload_part(
                        Bucket=BUCKET_NAME,
                        Key=s3_object_path,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )
                    
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part_response['ETag']
                    })
                    
                    uploaded_bytes += chunk_size
                    progress = (uploaded_bytes / file_size) * 100
                    log_message(f"✓ Part {part_number} uploaded. Progress: {progress:.1f}%")
                    
                    del chunk
                    part_number += 1
            
            log_message(f"Completing multipart upload ({len(parts)} parts)...")
            s3_client.complete_multipart_upload(
                Bucket=BUCKET_NAME,
                Key=s3_object_path,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            log_message(f"✓ MULTIPART UPLOAD SUCCESS")
            
            del parts
            gc.collect()
            
            return True, full_md5
            
        except Exception as e:
            log_message(f"ERROR during multipart upload: {e}")
            s3_client.abort_multipart_upload(
                Bucket=BUCKET_NAME,
                Key=s3_object_path,
                UploadId=upload_id
            )
            return False, None
            
    except Exception as e:
        log_message(f"ERROR: Failed to initiate multipart upload: {e}")
        return False, None


def upload_to_s3(local_file_path, s3_object_path, s3_client):
    """Загружает файл в S3"""
    try:
        if not os.path.exists(local_file_path):
            log_message(f"ERROR: Local file not found: {local_file_path}")
            return False, None, None
        
        file_size = os.path.getsize(local_file_path)
        
        if file_size > MULTIPART_THRESHOLD:
            success, md5_hex = upload_large_file_multipart(local_file_path, s3_object_path, s3_client)
            return success, md5_hex, "multipart"
        
        log_message(f"Uploading to S3: s3://{BUCKET_NAME}/{s3_object_path} ({file_size/1024/1024:.2f} MB)")
        
        md5_hash = calculate_md5(local_file_path)
        md5_digest = md5_hash.digest()
        md5_hex = md5_hash.hexdigest()
        md5_base64 = base64.b64encode(md5_digest).decode('utf-8')
        
        log_message(f"MD5: {md5_hex}")
        
        with open(local_file_path, 'rb') as f:
            response = s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_object_path,
                Body=f,
                ContentMD5=md5_base64
            )
        
        etag = response['ETag'].strip('"')
        status = response['ResponseMetadata']['HTTPStatusCode']
        
        del md5_hash, md5_digest
        gc.collect()
        
        if etag.lower() == md5_hex.lower():
            log_message(f"✓ UPLOAD SUCCESS (Status: {status})")
            return True, md5_hex, "standard"
        else:
            log_message(f"✗ MD5 mismatch! Expected: {md5_hex}, Got: {etag}")
            return False, md5_hex, "standard"
            
    except Exception as e:
        log_message(f"ERROR: Failed to upload to S3: {e}")
        return False, None, None


def cleanup_file(file_path):
    """Удаляет файл и пустые директории"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            log_message(f"✓ File deleted: {file_path}")
            
            parent_dir = Path(file_path).parent
            try:
                if parent_dir.exists() and not any(parent_dir.iterdir()) and str(TEMP_DOWNLOAD_DIR) in str(parent_dir):
                    parent_dir.rmdir()
                    log_message(f"✓ Empty directory deleted: {parent_dir}")
            except:
                pass
                
    except Exception as e:
        log_message(f"WARNING: Cannot delete file {file_path}: {e}")


def process_vd_folder_recursive(vd_hash, inner_path, vd_name, s3_base_path, token, s3_client, stats):
    """
    Рекурсивно обходит папку на виртуальном диске
    
    vd_hash: метка общего диска
    inner_path: путь внутри диска (например, "/" или "/Папка1")
    vd_name: имя диска для логов и S3
    s3_base_path: базовый путь в S3
    """
    log_message(f"\n{'='*70}")
    log_message(f"Processing VD folder: {inner_path}")
    log_message(f"VD: {vd_name} (hash: {vd_hash})")
    
    relative_path = inner_path.strip('/')
    s3_path = normalize_s3_path(s3_base_path, vd_name, relative_path)
    
    log_message(f"S3 destination: s3://{BUCKET_NAME}/{s3_path if s3_path else '(root)'}")
    log_message(f"{'='*70}")
    
    folder_start_time = time.time()
    
    metadata = get_vd_metadata(vd_hash, inner_path, token)
    
    if not metadata:
        log_message(f"ERROR: Cannot get metadata")
        write_csv_row([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            vd_name,
            vd_hash,
            'folder',
            os.path.basename(inner_path) if inner_path != '/' else 'root',
            inner_path,
            s3_path,
            0,
            0,
            'Metadata fetch failed',
            '-',
            '-',
            f"{time.time() - folder_start_time:.2f}",
            'Cannot get metadata'
        ])
        return
    
    if metadata.get('type') != 'dir':
        log_message(f"ERROR: Not a directory")
        return
    
    write_csv_row([
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        vd_name,
        vd_hash,
        'folder',
        metadata.get('name', 'root'),
        inner_path,
        s3_path,
        '-',
        '-',
        'Processed',
        '-',
        '-',
        f"{time.time() - folder_start_time:.2f}",
        ''
    ])
    
    items = metadata.get('_embedded', {}).get('items', [])
    log_message(f"Found {len(items)} items")
    
    del metadata
    
    for item in items:
        item_name = item.get('name')
        item_type = item.get('type')
        item_path_raw = item.get('path')
        item_size = item.get('size', 0)
        
        log_message(f"\n--- Processing: {item_name} (type: {item_type}) ---")
        log_message(f"    Raw path from API: {item_path_raw}")
        
        item_path = normalize_vd_path(item_path_raw)
        log_message(f"    Normalized VD path: {item_path}")
        
        item_inner_path = extract_inner_path(item_path)
        log_message(f"    Inner path: {item_inner_path}")
        
        if item_type == 'dir':
            stats['folders'] += 1
            process_vd_folder_recursive(
                vd_hash,
                item_inner_path,
                vd_name,
                s3_base_path,
                token,
                s3_client,
                stats
            )
            
        elif item_type == 'file':
            file_start_time = time.time()
            stats['files_total'] += 1
            
            item_inner_stripped = item_inner_path.strip('/')
            s3_object_path = normalize_s3_path(s3_base_path, vd_name, item_inner_stripped)
            
            safe_local_path = s3_object_path.lstrip('/')
            local_file_path = TEMP_DOWNLOAD_DIR / safe_local_path
            local_file_path_str = str(local_file_path)
            
            log_message(f"    Size: {item_size:,} bytes ({item_size/1024/1024:.2f} MB)")
            
            upload_method = "multipart" if item_size > MULTIPART_THRESHOLD else "standard"
            log_message(f"    Upload method: {upload_method.upper()}")
            log_message(f"    S3 path: {s3_object_path}")
            
            download_link = get_vd_download_link(item_path, token)
            
            if not download_link:
                error_msg = "Cannot get download link"
                log_message(f"    ERROR: {error_msg}")
                stats['files_failed'] += 1
                
                write_csv_row([
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    vd_name,
                    vd_hash,
                    'file',
                    item_name,
                    item_inner_path,
                    s3_object_path,
                    item_size,
                    f"{item_size/1024/1024:.2f}",
                    'Download link failed',
                    '-',
                    '-',
                    f"{time.time() - file_start_time:.2f}",
                    error_msg
                ])
                continue
            
            if not download_file(download_link, local_file_path_str):
                error_msg = "Download failed"
                stats['files_failed'] += 1
                
                write_csv_row([
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    vd_name,
                    vd_hash,
                    'file',
                    item_name,
                    item_inner_path,
                    s3_object_path,
                    item_size,
                    f"{item_size/1024/1024:.2f}",
                    'Download failed',
                    '-',
                    '-',
                    f"{time.time() - file_start_time:.2f}",
                    error_msg
                ])
                continue
            
            upload_success, md5_value, actual_method = upload_to_s3(local_file_path_str, s3_object_path, s3_client)
            
            if upload_success:
                status_text = 'Uploaded to S3'
                stats['files_uploaded'] += 1
                stats['bytes_uploaded'] += item_size
                error_msg = ''
            else:
                status_text = 'S3 upload failed'
                error_msg = 'Upload to S3 failed'
                stats['files_failed'] += 1
            
            duration = time.time() - file_start_time
            write_csv_row([
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                vd_name,
                vd_hash,
                'file',
                item_name,
                item_inner_path,
                s3_object_path,
                item_size,
                f"{item_size/1024/1024:.2f}",
                status_text,
                actual_method if actual_method else '-',
                md5_value if md5_value else '-',
                f"{duration:.2f}",
                error_msg
            ])
            
            cleanup_file(local_file_path_str)
            
            gc.collect()
            time.sleep(0.3)
    
    del items
    gc.collect()


def sync_virtual_disk_to_s3(vd_hash, vd_name, s3_base_folder=""):
    """
    Синхронизация виртуального диска в S3
    
    vd_hash: метка общего диска (обязательно!)
    vd_name: имя для логов и структуры папок в S3
    s3_base_folder: базовая папка в S3
    """
    log_message(f"\n{'#'*70}")
    log_message(f"STARTING SYNC: Virtual Disk → S3")
    log_message(f"{'#'*70}")
    log_message(f"VD Name:               {vd_name}")
    log_message(f"VD Hash:               {vd_hash}")
    log_message(f"Destination:           s3://{BUCKET_NAME}/{s3_base_folder if s3_base_folder else '(root)'}")
    log_message(f"Temp directory:        {TEMP_DOWNLOAD_DIR}")
    log_message(f"Multipart threshold:   {MULTIPART_THRESHOLD/1024/1024:.0f} MB")
    log_message(f"Log directory:         {LOG_DIR}")
    log_message(f"CSV file:              {CSV_FILE}")
    
    try:
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=AWS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=REGION
        )
        log_message("✓ S3 client initialized")
    except Exception as e:
        log_message(f"ERROR: Cannot create S3 client: {e}")
        return
    
    # Статистика
    stats = {
        'folders': 0,
        'files_total': 0,
        'files_uploaded': 0,
        'files_failed': 0,
        'bytes_uploaded': 0
    }
    
    start_time = time.time()
    
    # Обрабатываем виртуальный диск начиная с корня
    try:
        process_vd_folder_recursive(
            vd_hash,
            '/',  # Корень виртуального диска
            vd_name,
            s3_base_folder,
            YANDEX_ORG_TOKEN,
            s3_client,
            stats
        )
    except Exception as e:
        log_message(f"ERROR: Unexpected error: {e}")
        import traceback
        log_message(f"Traceback: {traceback.format_exc()}")
    
    # Очищаем временную директорию
    try:
        if TEMP_DOWNLOAD_DIR and TEMP_DOWNLOAD_DIR.exists():
            shutil.rmtree(TEMP_DOWNLOAD_DIR)
            log_message(f"✓ Cleaned up temp directory")
    except Exception as e:
        log_message(f"WARNING: Cannot cleanup temp directory: {e}")
    
    # Итоговая статистика
    elapsed_time = time.time() - start_time
    
    log_message(f"\n{'#'*70}")
    log_message(f"SYNC COMPLETED")
    log_message(f"{'#'*70}")
    log_message(f"Folders processed:      {stats['folders']}")
    log_message(f"Files total:            {stats['files_total']}")
    log_message(f"Files uploaded:         {stats['files_uploaded']}")
    log_message(f"Files failed:           {stats['files_failed']}")
    log_message(f"Bytes uploaded:         {stats['bytes_uploaded']:,} ({stats['bytes_uploaded']/1024/1024:.2f} MB)")
    log_message(f"Time elapsed:           {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    if stats['files_uploaded'] > 0 and elapsed_time > 0:
        log_message(f"Average speed:          {stats['bytes_uploaded']/elapsed_time/1024/1024:.2f} MB/s")
    log_message(f"CSV report:             {CSV_FILE}")
    if LOG_PART > 1:
        log_message(f"Log parts created:      {LOG_PART}")
    log_message(f"{'#'*70}\n")


if __name__ == '__main__':
    init_logging()
    
    log_message(f"\n{'#'*70}")
    log_message(f"YANDEX VIRTUAL DISK → S3 SYNC TOOL")
    log_message(f"{'#'*70}")
    log_message(f"Script started at:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_message(f"Script location:    {Path(__file__).parent.resolve()}")
    log_message(f"Configuration:")
    log_message(f"  VD Name:          {VD_NAME}")
    log_message(f"  VD Hash:          {VD_HASH}")
    log_message(f"  S3 Bucket:        {BUCKET_NAME}")
    log_message(f"  S3 Base folder:   {S3_BASE_FOLDER if S3_BASE_FOLDER else '(root)'}")
    log_message(f"  S3 Region:        {REGION}")
    log_message(f"  API Base:         {VIRTUAL_DISKS_API_BASE}")
    log_message(f"{'#'*70}\n")
    
    # Запускаем синхронизацию
    sync_virtual_disk_to_s3(VD_HASH, VD_NAME, S3_BASE_FOLDER)
