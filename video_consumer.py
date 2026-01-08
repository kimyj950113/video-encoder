# video_consumer.py (patched)
from dotenv import load_dotenv
import os
import time
from pathlib import Path
from typing import Optional, List, Dict, Tuple

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# .env 로드
load_dotenv()

# Google Drive
# 기존 drive.file -> drive 로 변경 (기존 폴더/파일을 못 보고 중복 생성되는 문제 완화)
SCOPES = ["https://www.googleapis.com/auth/drive"]
GDRIVE_ROOT_FOLDER_ID = "0ALadQkEfuBnGUk9PVA"
FOLDER_MIMETYPE = "application/vnd.google-apps.folder"

# 로컬 작업 폴더
LOCAL_WORKDIR = "./tmp_work"

# 폴링 주기(초): 새 파일 없을 때 쉬는 시간
POLL_INTERVAL_SECONDS = 60
UPLOAD_CHUNK_SIZE_MB = 16
UPLOAD_CHUNK_SIZE = UPLOAD_CHUNK_SIZE_MB * 1024 * 1024

# ===== Drive Query 안전 처리 =====
def _escape_drive_q(s: str) -> str:
    return s.replace("'", "\\'")

# ===== GDrive 공통 유틸 =====
def get_gdrive_service():
    creds = None
    token_path = "token.json"

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, "w") as token:
            token.write(creds.to_json())

    service = build("drive", "v3", credentials=creds)
    return service

def get_drive_id(service, root_folder_id: str) -> Optional[str]:
    """
    공유드라이브 안의 폴더/루트면 driveId가 내려올 수 있음.
    My Drive면 None일 수 있음.
    """
    meta = service.files().get(
        fileId=root_folder_id,
        fields="id,name,driveId",
        supportsAllDrives=True,
    ).execute()
    return meta.get("driveId")

def drive_list(service, q: str, fields: str, drive_id: Optional[str]):
    """
    공유드라이브면 corpora/driveId를 명시해서 list 누락을 줄임.
    기본 corpora는 'user'라 shared drive에서 원하는 결과가 안 나올 수 있음.
    """
    kwargs = dict(
        q=q,
        fields=fields,
        spaces="drive",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        pageSize=1000,
    )
    if drive_id:
        kwargs.update(dict(corpora="drive", driveId=drive_id))
    return service.files().list(**kwargs).execute()

def get_or_create_folder(service, parent_id: str, name: str, drive_id: Optional[str]) -> str:
    name_q = _escape_drive_q(name)
    query = (
        f"name = '{name_q}' and "
        f"mimeType = '{FOLDER_MIMETYPE}' and "
        f"'{parent_id}' in parents and trashed = false"
    )
    res = drive_list(
        service,
        q=query,
        fields="files(id, name)",
        drive_id=drive_id,
    )
    files = res.get("files", [])
    if files:
        if len(files) > 1:
            print(f"[WARN] 동일 폴더명 중복 감지: name={name} parent={parent_id} count={len(files)}")
        return files[0]["id"]

    file_metadata = {
        "name": name,
        "mimeType": FOLDER_MIMETYPE,
        "parents": [parent_id],
    }
    folder = service.files().create(
        body=file_metadata,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return folder["id"]

def get_or_create_path(service, root_id: str, parts: List[str], drive_id: Optional[str]) -> str:
    current_id = root_id
    for name in parts:
        current_id = get_or_create_folder(service, current_id, name, drive_id)
    return current_id

def find_file_in_folder(service, parent_id: str, name: str, drive_id: Optional[str]) -> Optional[str]:
    name_q = _escape_drive_q(name)
    query = (
        f"name = '{name_q}' and "
        f"'{parent_id}' in parents and trashed = false"
    )
    res = drive_list(
        service,
        q=query,
        fields="files(id, name)",
        drive_id=drive_id,
    )
    files = res.get("files", [])
    if files:
        if len(files) > 1:
            print(f"[WARN] 동일 파일명 중복 감지: name={name} parent={parent_id} count={len(files)}")
        return files[0]["id"]
    return None

def upload_file_to_gdrive(service, local_path: Path, parent_id: str, target_name: str, kind: str) -> str:
    """
    로컬 파일을 GDrive에 업로드하면서 10% 단위로 진행률 출력.
    kind: 'raw' 또는 'encoded' (로그에만 사용)
    """
    file_metadata = {
        "name": target_name,
        "parents": [parent_id],
    }
    media = MediaFileUpload(str(local_path), resumable=True, chunksize=UPLOAD_CHUNK_SIZE)

    request = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    )

    print(f"[GDRIVE-{kind}] 업로드 시작: {target_name}")
    response = None
    last_bucket = -1  # 0~10

    while response is None:
        status, response = request.next_chunk()
        if status is not None:
            percent = int(status.progress() * 100)
            bucket = percent // 10
            if bucket != last_bucket and 0 <= bucket <= 10:
                last_bucket = bucket
                print(f"[UPLOAD-{kind}][{target_name}] 진행률: {bucket * 10}%")

    file_id = response.get("id")
    print(f"[GDRIVE-{kind}] 업로드 완료: {target_name} (id={file_id})")
    return file_id

# ===== 업로드 파이프라인 (한 번 스캔) =====
def upload_tree_once(service, drive_id: Optional[str], root: Path, kind: str) -> dict:
    """
    root 아래 모든 파일을 한 번 순회하며 GDrive 업로드.
    root는 tmp_work/raw 또는 tmp_work/encoded.
    kind는 'raw' 또는 'encoded'.
    반환값: {'uploaded': x, 'skipped': y, 'failed': z}
    """
    stats = {"uploaded": 0, "skipped": 0, "failed": 0}

    if not root.exists():
        return stats

    # .part 파일은 준비 중인 파일이므로 무시
    all_files = [p for p in root.rglob("*") if p.is_file() and not p.name.endswith(".part")]
    all_files.sort()

    if not all_files:
        return stats

    print(f"[INFO-{kind}] 이번 스캔 업로드 대상 파일 수: {len(all_files)}")

    for f in all_files:
        rel = f.relative_to(root)
        parts = list(rel.parts)
        if not parts:
            continue

        filename = parts[-1]
        folder_parts = parts[:-1]

        print("\n==============================")
        print(f"[PIPELINE-UP-{kind}] 대상 파일")
        print(f"  - 로컬 경로         : {f}")
        print(f"  - GDrive 폴더경로   : /" + "/".join(folder_parts))
        print(f"  - 업로드 파일명     : {filename}")
        print("==============================")

        try:
            # 1) GDrive 폴더 경로 생성/탐색
            parent_id = get_or_create_path(service, GDRIVE_ROOT_FOLDER_ID, folder_parts, drive_id)

            # 2) 이미 있는지 확인
            existing_id = find_file_in_folder(service, parent_id, filename, drive_id)
            if existing_id:
                print(f"[SKIP-{kind}] GDrive에 이미 존재: {filename} (id={existing_id})")
                try:
                    f.unlink(missing_ok=True)
                    print(f"[CLEAN-{kind}] 로컬 파일 삭제(기존 존재): {f}")
                except PermissionError as e:
                    print(f"[BUSY-{kind}] 파일 사용 중이라 삭제 보류: {f} ({e})")
                except Exception as e:
                    print(f"[WARN-{kind}] 로컬 삭제 실패: {f} ({e})")
                stats["skipped"] += 1
                continue

            # 3) 업로드
            upload_file_to_gdrive(service, f, parent_id, filename, kind)
            stats["uploaded"] += 1

            # 4) 업로드 성공 후 로컬 삭제
            try:
                f.unlink(missing_ok=True)
                print(f"[CLEAN-{kind}] 로컬 파일 삭제(업로드 완료): {f}")
            except PermissionError as e:
                print(f"[BUSY-{kind}] 파일 사용 중이라 삭제 보류: {f} ({e})")
            except Exception as e:
                print(f"[WARN-{kind}] 로컬 삭제 실패: {f} ({e})")

        except Exception as e:
            stats["failed"] += 1
            print(f"[ERROR-{kind}] 업로드 실패: {f}")
            print(f"    {type(e).__name__}: {e}")

    print(f"\n[SUMMARY-ONCE-{kind}] 이번 스캔 결과")
    print(f"[SUMMARY-ONCE-{kind}] 업로드된 파일 수: {stats['uploaded']}")
    print(f"[SUMMARY-ONCE-{kind}] 이미 존재하여 스킵된 파일 수: {stats['skipped']}")
    print(f"[SUMMARY-ONCE-{kind}] 업로드 실패 파일 수: {stats['failed']}")
    return stats

def main():
    base_dir = Path(__file__).parent
    work_dir = (base_dir / LOCAL_WORKDIR).resolve()
    raw_root = work_dir / "raw"
    enc_root = work_dir / "encoded"

    print(f"[INFO] LOCAL_WORKDIR = {work_dir}")
    service = get_gdrive_service()
    drive_id = get_drive_id(service, GDRIVE_ROOT_FOLDER_ID)
    print(f"[INFO] driveId = {drive_id if drive_id else '(None - My Drive or unknown)'}")

    try:
        while True:
            print("\n==============================")
            print("[LOOP] 새 파일 스캔 시작")

            any_real_work = False

            raw_stats = upload_tree_once(service, drive_id, raw_root, "raw")
            if raw_stats["uploaded"] > 0 or raw_stats["failed"] > 0:
                any_real_work = True

            enc_stats = upload_tree_once(service, drive_id, enc_root, "encoded")
            if enc_stats["uploaded"] > 0 or enc_stats["failed"] > 0:
                any_real_work = True

            if not any_real_work:
                print(f"[IDLE] 업로드할 새 파일이 없습니다. {POLL_INTERVAL_SECONDS}초 대기 후 재스캔...")
                time.sleep(POLL_INTERVAL_SECONDS)
            else:
                print("[LOOP] 이번 스캔에서 실제 업로드/실패가 있었으므로 바로 다음 스캔으로 진행합니다.")

    except KeyboardInterrupt:
        print("\n[EXIT] 사용자 중단(Ctrl+C)으로 업로드 모니터링을 종료합니다.")

if __name__ == "__main__":
    main()
