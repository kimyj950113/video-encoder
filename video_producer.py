from dotenv import load_dotenv
import os
import re
import subprocess
import shutil
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import time

import dropbox
from dropbox.files import FileMetadata

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ===== GDrive API 재시도 설정 =====
GDRIVE_API_MAX_RETRIES = 3
GDRIVE_API_RETRY_DELAY = 5

# .env에서 환경변수 로드
load_dotenv()

# ===== 기본 설정 =====
TARGET_SIZE_MB = 512
SAFETY_MARGIN = 0.95
MAX_RETRIES = 3

# Dropbox OAuth (refresh token 기반)
DBX_APP_KEY = os.getenv("DBX_APP_KEY")
DBX_APP_SECRET = os.getenv("DBX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")

# Dropbox: /디자인 아래 전체 탐색
DROPBOX_ROOT = "/디자인/UIUX 디자인/TAC_니카"

# 로컬 작업 폴더 (임시 다운로드/인코딩)
LOCAL_WORKDIR = "./tmp_work"

# ffmpeg / ffprobe 실행 파일 이름 (PATH에 등록된 상태 기준)
FFMPEG_BIN = "ffmpeg"
FFPROBE_BIN = "ffprobe"

# 실제 인코딩 대신 로그만 찍는 모드 (테스트용)
DRY_RUN = False

# Google Drive
# 기존 drive.file -> drive 로 변경 (기존 폴더/파일 조회 누락 방지)
SCOPES = ["https://www.googleapis.com/auth/drive"]
GDRIVE_ROOT_FOLDER_ID = "0ALadQkEfuBnGUk9PVA"
FOLDER_MIMETYPE = "application/vnd.google-apps.folder"


# ===== 공통 유틸 =====
def _execute_gdrive_with_retry(request, desc: str = ""):
    for attempt in range(1, GDRIVE_API_MAX_RETRIES + 1):
        try:
            return request.execute()
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            print(f"[GDRIVE-RETRY] {desc} 시도 {attempt}/{GDRIVE_API_MAX_RETRIES} 실패: {e}")
            if attempt >= GDRIVE_API_MAX_RETRIES:
                print(f"[GDRIVE-RETRY] {desc} 재시도 한계 도달 → 예외 전파")
                raise
            time.sleep(GDRIVE_API_RETRY_DELAY)


def _escape_drive_q(s: str) -> str:
    return s.replace("'", "\\'")


def run_cmd(cmd: List[str]) -> str:
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{result.stdout}")
    return result.stdout


def get_video_duration(path: Path) -> float:
    cmd = [
        FFPROBE_BIN,
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    out = run_cmd(cmd).strip()
    return float(out)


def calc_bitrate_kbps(duration_sec: float, target_size_mb: int, safety_margin: float = 0.9) -> int:
    target_size_bytes = target_size_mb * 1024 * 1024 * safety_margin
    total_bits = target_size_bytes * 8
    bps = total_bits / duration_sec
    kbps = int(bps / 1000)
    return kbps


time_pattern = re.compile(r"time=(\d+):(\d+):(\d+\.\d+)")


def _parse_ffmpeg_time_to_seconds(line: str) -> Optional[float]:
    m = time_pattern.search(line)
    if not m:
        return None
    h = int(m.group(1))
    m_ = int(m.group(2))
    s = float(m.group(3))
    return h * 3600 + m_ * 60 + s


def encode_video_to_target_size(input_path: Path, output_path: Path, target_size_mb: int) -> None:
    duration = get_video_duration(input_path)
    total_kbps = calc_bitrate_kbps(duration, target_size_mb, SAFETY_MARGIN)

    v_bitrate = max(int(total_kbps * 0.8), 300)
    a_bitrate = max(int(total_kbps * 0.2), 64)

    print(f"[ENCODE] {input_path.name}: duration={duration:.1f}s, total~{total_kbps}kbps (v={v_bitrate}, a={a_bitrate})")

    cmd = [
        FFMPEG_BIN,
        "-y",
        "-i", str(input_path),
        "-vf", "scale=-2:1080",
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-b:v", f"{v_bitrate}k",
        "-c:a", "aac",
        "-b:a", f"{a_bitrate}k",
        "-movflags", "+faststart",
        "-f", "mp4",
        str(output_path),
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="ignore",
        bufsize=1,
    )

    last_bucket = -1
    try:
        assert process.stdout is not None
        for line in process.stdout:
            line = line.rstrip("\n")
            t = _parse_ffmpeg_time_to_seconds(line)
            if t is not None and duration > 0:
                percent = int((t / duration) * 100)
                bucket = percent // 10
                if bucket != last_bucket and 0 <= bucket <= 10:
                    last_bucket = bucket
                    print(f"[ENCODE][{input_path.name}] 진행률: {bucket * 10}% ({t:.1f}s / {duration:.1f}s)")
        process.wait()
    finally:
        if process.poll() is None:
            process.kill()

    if process.returncode != 0:
        raise RuntimeError(f"ffmpeg 인코딩 실패 (returncode={process.returncode})")

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"[ENCODE] result size = {size_mb:.1f} MB")


# ===== 파일명 / 폴더 경로 변환 =====
def flat_name_from_dropbox_path(path_display: str) -> str:
    p = path_display.strip("/")
    parts = p.split("/")

    *dirs, filename = parts
    name_without_ext, _ = os.path.splitext(filename)

    dirs = [d for d in dirs if d != "최종편집영상"]
    all_parts = dirs + [name_without_ext]
    flat = "_".join(all_parts)
    return flat + ".mp4"


def encoded_rel_folder_from_dropbox(path_display: str) -> list[str]:
    parts = [p for p in path_display.split("/") if p]
    if "최종편집영상" not in parts:
        raise ValueError(f"'최종편집영상' 폴더를 찾을 수 없습니다: {path_display}")
    idx = parts.index("최종편집영상")
    prefix = parts[:idx]
    return prefix + ["encoded"]


def raw_rel_path_from_dropbox(path_display: str) -> Path:
    p = path_display.lstrip("/")
    return Path(p)


# ===== Dropbox 관련 =====
def get_dropbox_client() -> dropbox.Dropbox:
    app_key = DBX_APP_KEY
    app_secret = DBX_APP_SECRET
    refresh_token = DROPBOX_REFRESH_TOKEN

    if not app_key or not app_secret or not refresh_token:
        raise RuntimeError("DBX_APP_KEY / DBX_APP_SECRET / DROPBOX_REFRESH_TOKEN 환경변수를 확인하세요.")

    dbx = dropbox.Dropbox(
        app_key=app_key,
        app_secret=app_secret,
        oauth2_refresh_token=refresh_token,
    )
    return dbx


def list_dropbox_files_recursive(dbx: dropbox.Dropbox, root_path: str) -> List[FileMetadata]:
    res = dbx.files_list_folder(root_path, recursive=True)

    entries = list(res.entries)
    while res.has_more:
        res = dbx.files_list_folder_continue(res.cursor)
        entries.extend(res.entries)

    files: List[FileMetadata] = [e for e in entries if isinstance(e, FileMetadata)]
    return files


# ===== Google Drive 관련 (조회 전용) =====
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
    meta = service.files().get(
        fileId=root_folder_id,
        fields="id,name,driveId",
        supportsAllDrives=True,
    ).execute()
    return meta.get("driveId")


def drive_list(service, q: str, fields: str, drive_id: Optional[str]):
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
    req = service.files().list(**kwargs)
    return _execute_gdrive_with_retry(req, desc=f"list q={q[:80]}...")


def get_folder_if_exists(service, parent_id: str, name: str, drive_id: Optional[str]) -> Optional[str]:
    name_q = _escape_drive_q(name)
    query = (
        f"name = '{name_q}' and "
        f"mimeType = '{FOLDER_MIMETYPE}' and "
        f"'{parent_id}' in parents and trashed = false"
    )
    res = drive_list(service, q=query, fields="files(id, name)", drive_id=drive_id)
    files = res.get("files", [])
    if files:
        if len(files) > 1:
            print(f"[WARN] 동일 폴더명 중복 감지: name={name} parent={parent_id} count={len(files)}")
        return files[0]["id"]
    return None


def get_path_if_exists(service, root_id: str, parts: List[str], drive_id: Optional[str]) -> Optional[str]:
    current_id = root_id
    for name in parts:
        folder_id = get_folder_if_exists(service, current_id, name, drive_id)
        if not folder_id:
            return None
        current_id = folder_id
    return current_id


def find_file_in_folder(service, parent_id: str, name: str, drive_id: Optional[str]) -> Optional[str]:
    name_q = _escape_drive_q(name)
    query = (
        f"name = '{name_q}' and "
        f"'{parent_id}' in parents and trashed = false"
    )
    res = drive_list(service, q=query, fields="files(id, name)", drive_id=drive_id)
    files = res.get("files", [])
    if files:
        if len(files) > 1:
            print(f"[WARN] 동일 파일명 중복 감지: name={name} parent={parent_id} count={len(files)}")
        return files[0]["id"]
    return None


def find_gdrive_file_by_path(
    service,
    root_id: str,
    drive_id: Optional[str],
    folder_parts: List[str],
    filename: str
) -> Optional[str]:
    parent_id = get_path_if_exists(service, root_id, folder_parts, drive_id)
    if not parent_id:
        return None
    return find_file_in_folder(service, parent_id, filename, drive_id)


# ===== 메인 파이프라인 (준비 전용) =====
def process_all():
    base_dir = Path(__file__).parent
    work_dir = (base_dir / LOCAL_WORKDIR).resolve()
    raw_root = work_dir / "raw"
    enc_root = work_dir / "encoded"
    raw_root.mkdir(parents=True, exist_ok=True)
    enc_root.mkdir(parents=True, exist_ok=True)

    # 이전 실행에서 남은 .part 임시파일 정리
    for part_file in list(raw_root.rglob("*.part")) + list(enc_root.rglob("*.part")):
        try:
            print(f"[CLEANUP] 이전 실행에서 남은 임시 파일 삭제: {part_file}")
            part_file.unlink()
        except Exception as e:
            print(f"[CLEANUP-WARN] 임시 파일 삭제 실패: {part_file} ({e})")

    dbx = get_dropbox_client()
    gdrive = get_gdrive_service()
    drive_id = get_drive_id(gdrive, GDRIVE_ROOT_FOLDER_ID)
    print(f"[INFO] driveId = {drive_id if drive_id else '(None - My Drive or unknown)'}")

    dbx_files = list_dropbox_files_recursive(dbx, DROPBOX_ROOT)
    dbx_files = sorted(dbx_files, key=lambda meta: meta.path_display)

    print(f"[INFO] Found {len(dbx_files)} entries under {DROPBOX_ROOT}")
    print(f"[INFO] DRY_RUN = {DRY_RUN}")
    print(f"[INFO] LOCAL_WORKDIR = {work_dir}")

    video_exts = {".mp4", ".mov", ".mkv", ".avi", ".wmv"}

    total_targets = 0
    skipped_closed = 0
    skipped_existing_local = 0
    skipped_existing_gdrive = 0
    failed_files = 0

    for meta in dbx_files:
        path_display = meta.path_display
        ext = Path(path_display).suffix.lower()

        if "(폐강" in path_display:
            skipped_closed += 1
            print(f"[SKIP] (폐강) 포함 경로 -> 준비 안 함: {path_display}")
            continue

        if ext not in video_exts:
            continue

        parts = path_display.split("/")
        if "최종편집영상" not in parts:
            continue

        total_targets += 1

        raw_rel_path = raw_rel_path_from_dropbox(path_display)
        raw_local_path = raw_root / raw_rel_path
        encoded_folder_rel_parts = encoded_rel_folder_from_dropbox(path_display)
        flat_name = flat_name_from_dropbox_path(path_display)
        encoded_local_path = enc_root.joinpath(*encoded_folder_rel_parts) / flat_name

        print("\n==============================")
        print(f"[PIPELINE-PREP] 대상 파일")
        print(f"  - Dropbox 경로      : {path_display}")
        print(f"  - Raw 로컬경로      : {raw_local_path}")
        print(f"  - Encoded 로컬경로  : {encoded_local_path}")
        print("==============================")

        if raw_local_path.exists() and encoded_local_path.exists():
            skipped_existing_local += 1
            print(f"[SKIP-LOCAL] raw/encoded 모두 로컬에 이미 존재 -> 준비 스킵: {path_display}")
            continue

        gdrive_folder_parts = encoded_folder_rel_parts
        try:
            existing_gdrive_id = find_gdrive_file_by_path(
                gdrive, GDRIVE_ROOT_FOLDER_ID, drive_id, gdrive_folder_parts, flat_name
            )
        except Exception as e:
            failed_files += 1
            print(f"[WARN-GDRIVE] GDrive에서 기존 encoded 조회 중 오류 -> 이 파일은 준비 실패로 건너뜀")
            print(f"             경로: /" + "/".join(gdrive_folder_parts) + f"/{flat_name}")
            print(f"             {type(e).__name__}: {e}")
            continue

        if existing_gdrive_id:
            skipped_existing_gdrive += 1
            print(f"[SKIP-GDRIVE] GDrive에 encoded 파일 이미 존재 -> 준비/인코딩 스킵")
            print(f"             경로: /" + "/".join(gdrive_folder_parts) + f"/{flat_name}")
            print(f"             id: {existing_gdrive_id}")
            continue

        if DRY_RUN:
            print(f"[DRY RUN] 이 파일을 다음 순서로 준비할 예정입니다:")
            print(f"[DRY RUN]  1) Dropbox에서 다운로드 -> {raw_local_path}")
            print(f"[DRY RUN]  2) 원본 파일 크기 검사 -> {TARGET_SIZE_MB}MB 기준")
            print(f"[DRY RUN]  3) 필요시 인코딩 -> {encoded_local_path} (1080p, {TARGET_SIZE_MB}MB 타겟)")
            print(f"[DRY RUN]  4) 원본/인코딩본 모두 tmp_work에 남김 (삭제 안 함)")
            continue

        attempts = 0
        success = False
        last_error: Optional[Exception] = None
        raw_prepared = raw_local_path.exists()

        while attempts < MAX_RETRIES and not success:
            attempts += 1
            print(f"[ATTEMPT-PREP] {flat_name} - {attempts}/{MAX_RETRIES} 시도")

            try:
                raw_local_path.parent.mkdir(parents=True, exist_ok=True)
                encoded_local_path.parent.mkdir(parents=True, exist_ok=True)

                tmp_raw = raw_local_path.with_suffix(raw_local_path.suffix + ".part")
                tmp_enc = encoded_local_path.with_suffix(encoded_local_path.suffix + ".part")

                for p in (tmp_raw, tmp_enc):
                    if p.exists():
                        try:
                            print(f"[CLEANUP] 기존 임시 파일 삭제: {p}")
                            p.unlink()
                        except Exception as ee:
                            print(f"[CLEANUP-WARN] 임시 파일 삭제 실패: {p} ({ee})")

                if not raw_prepared:
                    print(f"[STEP] Dropbox에서 다운로드 중 -> {tmp_raw}")
                    with open(tmp_raw, "wb") as f:
                        _, res = dbx.files_download(path=path_display)
                        f.write(res.content)
                    print(f"[STEP] 다운로드 완료")
                    tmp_raw.replace(raw_local_path)
                    print(f"[STEP] raw 파일 준비 완료: {raw_local_path}")
                    raw_prepared = True
                else:
                    print(f"[STEP] raw 이미 존재, 재다운로드 생략: {raw_local_path}")

                orig_size_mb = raw_local_path.stat().st_size / (1024 * 1024)
                print(f"[CHECK] 원본 파일 크기 = {orig_size_mb:.1f} MB (TARGET={TARGET_SIZE_MB} MB)")

                if orig_size_mb <= TARGET_SIZE_MB:
                    print(f"[INFO] 원본이 목표 용량 이하 -> 인코딩 생략, Encoded용 파일 복사 생성")
                    shutil.copy2(raw_local_path, tmp_enc)
                else:
                    print(f"[STEP] ffmpeg 인코딩 시작 -> {tmp_enc}")
                    encode_video_to_target_size(raw_local_path, tmp_enc, TARGET_SIZE_MB)

                tmp_enc.replace(encoded_local_path)
                print(f"[STEP] encoded 파일 준비 완료: {encoded_local_path}")

                success = True

            except Exception as e:
                last_error = e
                print(f"[ERROR] 준비 시도 {attempts}/{MAX_RETRIES} 중 오류 발생: {path_display}")
                print(f"        {type(e).__name__}: {e}")
                if attempts < MAX_RETRIES:
                    print(f"[RETRY] {flat_name} 준비 다시 시도 예정...")
                else:
                    print(f"[GIVEUP] {flat_name} - 최대 재시도 횟수 초과, 이 파일 준비 건너뜀.")
                    failed_files += 1

        if success:
            print(f"[PREP-DONE] 준비 완료 (raw + encoded 로컬에 존재): {path_display}")
        else:
            print(f"[PREP-FAIL] 최종 실패 파일: {path_display}")
            if last_error:
                print(f"      마지막 에러: {type(last_error).__name__}: {last_error}")

    print(f"\n[PREP-SUMMARY] 총 대상 파일 수: {total_targets}")
    print(f"[PREP-SUMMARY] (폐강)으로 스킵된 파일 수: {skipped_closed}")
    print(f"[PREP-SUMMARY] 로컬 준비 완료 상태로 스킵된 파일 수: {skipped_existing_local}")
    print(f"[PREP-SUMMARY] GDrive에 이미 encoded가 있어 스킵된 파일 수: {skipped_existing_gdrive}")
    print(f"[PREP-SUMMARY] 준비 단계에서 최종 실패한 파일 수: {failed_files}")
    if DRY_RUN:
        print("[PREP-SUMMARY] DRY_RUN 모드: 실제 다운로드/인코딩은 수행되지 않았습니다.")


if __name__ == "__main__":
    process_all()
