from dotenv import load_dotenv
import os
import re
import subprocess
import shutil
from pathlib import Path
from typing import List, Optional, Tuple
from queue import Queue
from threading import Thread

import dropbox
from dropbox.files import FileMetadata

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# .env에서 환경변수 로드
load_dotenv()

# ===== 기본 설정 =====
TARGET_SIZE_MB = 512        # 목표 파일 크기 (MB)
SAFETY_MARGIN = 0.95        # 여유율 (0.95면 5% 정도 더 작게)
MAX_RETRIES = 3             # 파일별 최대 재시도 횟수 (다운로드/인코딩)
MAX_UPLOAD_RETRIES = 3      # 업로드 재시도 횟수

# Dropbox OAuth (refresh token 기반)
DBX_APP_KEY = os.getenv("DBX_APP_KEY")
DBX_APP_SECRET = os.getenv("DBX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")

# Dropbox: /디자인 아래 전체 탐색
DROPBOX_ROOT = "/디자인"

# 로컬 작업 폴더 (임시 다운로드/인코딩)
LOCAL_WORKDIR = "./tmp_work"

# ffmpeg / ffprobe 실행 파일 이름 (PATH에 등록된 상태 기준)
FFMPEG_BIN = "ffmpeg"
FFPROBE_BIN = "ffprobe"

# 실제 인코딩/업로드 대신 로그만 찍는 모드 (테스트용)
DRY_RUN = False   # 테스트만 하고 싶으면 True 로 바꾸기

# Google Drive
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# 공유드라이브 루트 폴더 ID
GDRIVE_ROOT_FOLDER_ID = "0ALadQkEfuBnGUk9PVA"

# 업로드 워커 스레드 개수
UPLOAD_WORKERS = 2


# ===== 공통 유틸 =====
def run_cmd(cmd: List[str]) -> str:
    """subprocess로 외부 명령 실행 (ffmpeg/ffprobe용, UTF-8 로그 대응)"""
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
    """ffprobe로 동영상 길이(초) 가져오기"""
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
    """목표 용량(MB)과 길이(초) 기준으로 전체 비트레이트(kbps) 계산"""
    target_size_bytes = target_size_mb * 1024 * 1024 * safety_margin
    total_bits = target_size_bytes * 8
    bps = total_bits / duration_sec
    kbps = int(bps / 1000)
    return kbps


# ffmpeg 진행률 파싱용
time_pattern = re.compile(r"time=(\d+):(\d+):(\d+\.\d+)")


def _parse_ffmpeg_time_to_seconds(line: str) -> Optional[float]:
    """
    ffmpeg 로그 라인에서 time=HH:MM:SS.ss 를 찾아서 초(float)로 변환
    예: '... time=00:10:32.45 bitrate=...' -> 632.45
    """
    m = time_pattern.search(line)
    if not m:
        return None
    h = int(m.group(1))
    m_ = int(m.group(2))
    s = float(m.group(3))
    return h * 3600 + m_ * 60 + s


def encode_video_to_target_size(input_path: Path, output_path: Path, target_size_mb: int) -> None:
    """입력 동영상을 target_size_mb 이하가 되도록 재인코딩 + 1080p 제한 + ffmpeg 진행률(10% 단위) 표시"""
    duration = get_video_duration(input_path)
    total_kbps = calc_bitrate_kbps(duration, target_size_mb, SAFETY_MARGIN)

    # 비디오 80%, 오디오 20% 비중, 너무 낮아지는 것 방지용 최소값
    v_bitrate = max(int(total_kbps * 0.8), 300)
    a_bitrate = max(int(total_kbps * 0.2), 64)

    print(f"[ENCODE] {input_path.name}: duration={duration:.1f}s, total~{total_kbps}kbps (v={v_bitrate}, a={a_bitrate})")

    # 1080p로 리사이즈: 세로 1080, 가로는 비율 유지(-2)
    cmd = [
        FFMPEG_BIN,
        "-y",
        "-i", str(input_path),
        "-vf", "scale=-2:1080",
        "-c:v", "libx264",
        "-preset", "veryfast",  # 속도/품질 균형용
        "-b:v", f"{v_bitrate}k",
        "-c:a", "aac",
        "-b:a", f"{a_bitrate}k",
        "-movflags", "+faststart",
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

    last_bucket = -1  # 0~10 (0~100%)
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
    """
    예:
      '/디자인/2D 모션그래픽 디자인/TAC_장준석/최종편집영상/W4/2.mp4'
    -> '디자인_2D 모션그래픽 디자인_TAC_장준석_W4_2.mp4'
    """
    p = path_display.strip("/")
    parts = p.split("/")

    *dirs, filename = parts
    name_without_ext, _ = os.path.splitext(filename)

    # '최종편집영상' 폴더는 파일명에서 제외
    dirs = [d for d in dirs if d != "최종편집영상"]

    all_parts = dirs + [name_without_ext]
    flat = "_".join(all_parts)

    return flat + ".mp4"


def gdrive_folder_parts_from_dropbox(path_display: str) -> list[str]:
    """
    인코딩본 저장용 GDrive 폴더 경로:
      '/디자인/그래픽디자인/MPC_임수연/최종편집영상/W1/2.mp4'
    -> ['디자인', '그래픽디자인', 'MPC_임수연', 'encoded']
    """
    parts = [p for p in path_display.split("/") if p]

    if "최종편집영상" not in parts:
        raise ValueError(f"'최종편집영상' 폴더를 찾을 수 없습니다: {path_display}")

    idx = parts.index("최종편집영상")
    prefix = parts[:idx]

    return prefix + ["encoded"]


def gdrive_raw_folder_parts_from_dropbox(path_display: str) -> list[str]:
    """
    원본 저장용 GDrive 폴더 경로:
      '/디자인/그래픽디자인/MPC_임수연/최종편집영상/W1/2.mp4'
    -> ['디자인', '그래픽디자인', 'MPC_임수연', '최종편집영상', 'W1']
    (Dropbox와 동일한 폴더 구조를 미러링)
    """
    parts = [p for p in path_display.split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"경로가 너무 짧습니다: {path_display}")
    return parts[:-1]  # 마지막 파일명만 제거


# ===== Dropbox 관련 =====
def get_dropbox_client() -> dropbox.Dropbox:
    """
    refresh token 기반 Dropbox 클라이언트 생성.
    SDK가 내부적으로 access token을 자동 갱신.
    """
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
    """root_path 아래를 재귀로 훑어서 FileMetadata만 반환"""
    res = dbx.files_list_folder(root_path, recursive=True)

    entries = list(res.entries)
    while res.has_more:
        res = dbx.files_list_folder_continue(res.cursor)
        entries.extend(res.entries)

    files: List[FileMetadata] = [e for e in entries if isinstance(e, FileMetadata)]
    return files


# ===== Google Drive 관련 =====
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


def get_or_create_folder(service, parent_id: str, name: str) -> str:
    query = (
        f"name = '{name}' and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"'{parent_id}' in parents and trashed = false"
    )
    res = service.files().list(
        q=query,
        fields="files(id, name)",
        spaces="drive",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]

    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = service.files().create(
        body=file_metadata,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return folder["id"]


def get_or_create_path(service, root_id: str, parts: list[str]) -> str:
    current_id = root_id
    for name in parts:
        current_id = get_or_create_folder(service, current_id, name)
    return current_id


def find_file_in_folder(service, parent_id: str, name: str) -> Optional[str]:
    query = (
        f"name = '{name}' and "
        f"'{parent_id}' in parents and trashed = false"
    )
    res = service.files().list(
        q=query,
        fields="files(id, name)",
        spaces="drive",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    files = res.get("files", [])
    if files:
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
    media = MediaFileUpload(str(local_path), resumable=True)

    request = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    )

    print(f"[GDRIVE-{kind}] 업로드 시작: {target_name}")
    response = None
    last_bucket = -1  # 0~10 (0~100%)

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


# ===== 업로드 워커 =====
UploadTask = Tuple[str, str, str, str]  # (local_path, parent_id, filename, kind)
STOP_SENTINEL: UploadTask = ("", "", "", "STOP")


def uploader_worker(service, q: Queue):
    while True:
        local_path, parent_id, filename, kind = q.get()
        if kind == "STOP":
            q.task_done()
            break

        path_obj = Path(local_path)
        if not path_obj.exists():
            print(f"[UPLOAD-{kind}][WARN] 로컬 파일이 존재하지 않습니다: {local_path}")
            q.task_done()
            continue

        success = False
        last_error: Optional[Exception] = None

        for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
            try:
                upload_file_to_gdrive(service, path_obj, parent_id, filename, kind)
                success = True
                break
            except Exception as e:
                last_error = e
                print(f"[UPLOAD-{kind}][ERROR] {filename} 업로드 시도 {attempt}/{MAX_UPLOAD_RETRIES} 실패")
                print(f"    {type(e).__name__}: {e}")

        if not success:
            print(f"[UPLOAD-{kind}][GIVEUP] {filename} 업로드 포기")
        else:
            # 업로드 성공 시 로컬 파일 삭제
            try:
                path_obj.unlink(missing_ok=True)
                print(f"[UPLOAD-{kind}] 로컬 파일 삭제: {local_path}")
            except Exception as e:
                print(f"[UPLOAD-{kind}][WARN] 로컬 파일 삭제 실패: {local_path} ({e})")

        q.task_done()


# ===== 전체 파이프라인 =====
def process_all():
    base_dir = Path(__file__).parent
    work_dir = base_dir / LOCAL_WORKDIR
    work_dir.mkdir(parents=True, exist_ok=True)

    # 시작 시, 이전 실행에서 남은 임시파일 정리
    for f in work_dir.glob("*"):
        try:
            f.unlink()
        except OSError:
            pass

    dbx = get_dropbox_client()
    gdrive = get_gdrive_service()

    # Dropbox 파일 목록 가져와서 path_display 기준으로 정렬 (가나다/알파벳 순)
    dbx_files = list_dropbox_files_recursive(dbx, DROPBOX_ROOT)
    dbx_files = sorted(dbx_files, key=lambda meta: meta.path_display)

    print(f"[INFO] Found {len(dbx_files)} entries under {DROPBOX_ROOT}")
    print(f"[INFO] DRY_RUN = {DRY_RUN}")

    video_exts = {".mp4", ".mov", ".mkv", ".avi", ".wmv"}

    total_targets = 0
    skipped_closed = 0
    skipped_existing_encoded = 0
    skipped_existing_raw = 0
    failed_files = 0

    # 업로드 큐 및 워커 스레드 준비
    upload_queue: Queue[UploadTask] = Queue()
    workers: list[Thread] = []
    for _ in range(UPLOAD_WORKERS):
        t = Thread(target=uploader_worker, args=(gdrive, upload_queue), daemon=True)
        t.start()
        workers.append(t)

    for meta in dbx_files:
        path_display = meta.path_display
        ext = Path(path_display).suffix.lower()

        # (폐강) 포함 경로는 아예 스킵
        if "(폐강" in path_display:
            skipped_closed += 1
            print(f"[SKIP] (폐강) 포함 경로 -> 인코딩/업로드 안 함: {path_display}")
            continue

        # 영상 파일만
        if ext not in video_exts:
            continue

        # '최종편집영상' 폴더 아래만 대상
        parts = path_display.split("/")
        if "최종편집영상" not in parts:
            continue

        total_targets += 1

        # 인코딩본 파일명 (flat)
        flat_name = flat_name_from_dropbox_path(path_display)
        # 원본 파일명 (드롭박스와 동일)
        raw_name = Path(path_display).name

        # GDrive 폴더 경로
        encoded_folder_parts = gdrive_folder_parts_from_dropbox(path_display)
        raw_folder_parts = gdrive_raw_folder_parts_from_dropbox(path_display)

        print("\n==============================")
        print(f"[PIPELINE] 대상 파일")
        print(f"  - Dropbox 경로      : {path_display}")
        print(f"  - Encoded 폴더경로  : /" + "/".join(encoded_folder_parts))
        print(f"  - Raw 폴더경로      : /" + "/".join(raw_folder_parts))
        print(f"  - Encoded 파일명    : {flat_name}")
        print(f"  - Raw 파일명        : {raw_name}")
        print("==============================")

        if DRY_RUN:
            local_in = work_dir / raw_name
            local_out = work_dir / ("encoded_" + flat_name)

            print(f"[DRY RUN] 이 파일을 다음 순서로 처리할 예정입니다:")
            print(f"[DRY RUN]  1) Dropbox에서 다운로드 -> {local_in}")
            print(f"[DRY RUN]  2) 원본 파일 크기 검사 -> {TARGET_SIZE_MB}MB 이하이면 인코딩 없이 Encoded 처리")
            print(f"[DRY RUN]  3) Encoded: /" + "/".join(encoded_folder_parts) + f"/{flat_name}")
            print(f"[DRY RUN]  4) Raw:     /" + "/".join(raw_folder_parts) + f"/{raw_name}")
            print(f"[DRY RUN]  5) 업로드 후 로컬 임시 파일 삭제")

            for bucket in range(0, 11):
                print(f"[DRY RUN][ENCODE-{flat_name}] 인코딩 진행률(개념): {bucket * 10}%")
                print(f"[DRY RUN][UPLOAD-encoded-{flat_name}] 업로드 진행률(개념): {bucket * 10}%")
                print(f"[DRY RUN][UPLOAD-raw-{raw_name}] 업로드 진행률(개념): {bucket * 10}%")

            continue

        # ===== 실제 작업: 재시도 로직 포함 (다운로드/인코딩) =====
        attempts = 0
        success = False
        last_error: Optional[Exception] = None

        while attempts < MAX_RETRIES and not success:
            attempts += 1
            print(f"[ATTEMPT] {flat_name} - {attempts}/{MAX_RETRIES} 시도")

            local_in = work_dir / raw_name
            local_out = work_dir / ("encoded_" + flat_name)

            try:
                # 1) GDrive 폴더 경로 생성/탐색 (encoded / raw 각각)
                print(f"[STEP] GDrive Encoded 폴더 경로 생성/탐색: /" + "/".join(encoded_folder_parts))
                gdrive_encoded_parent_id = get_or_create_path(gdrive, GDRIVE_ROOT_FOLDER_ID, encoded_folder_parts)

                print(f"[STEP] GDrive Raw 폴더 경로 생성/탐색: /" + "/".join(raw_folder_parts))
                gdrive_raw_parent_id = get_or_create_path(gdrive, GDRIVE_ROOT_FOLDER_ID, raw_folder_parts)

                # 2) 이미 있는지 확인
                encoded_exists_id = find_file_in_folder(gdrive, gdrive_encoded_parent_id, flat_name)
                raw_exists_id = find_file_in_folder(gdrive, gdrive_raw_parent_id, raw_name)

                encoded_required = encoded_exists_id is None
                raw_required = raw_exists_id is None

                if not encoded_required and not raw_required:
                    print(f"[SKIP] Encoded/Raw 모두 GDrive에 이미 존재 -> 스킵: {path_display}")
                    skipped_existing_encoded += 1
                    skipped_existing_raw += 1
                    success = True
                    break

                if not encoded_required and raw_required:
                    print(f"[INFO] Encoded는 이미 존재, Raw만 업로드 필요")
                elif encoded_required and not raw_required:
                    print(f"[INFO] Raw는 이미 존재, Encoded만 업로드 필요")
                else:
                    print(f"[INFO] Encoded + Raw 모두 업로드 필요")

                # 3) Dropbox -> 로컬 다운로드 (어느 한쪽이라도 필요하면)
                print(f"[STEP] Dropbox에서 다운로드 중 -> {local_in}")
                with open(local_in, "wb") as f:
                    _, res = dbx.files_download(path=path_display)
                    f.write(res.content)
                print(f"[STEP] 다운로드 완료")

                # 4) 원본 파일 크기 검사
                orig_size_mb = local_in.stat().st_size / (1024 * 1024)
                print(f"[CHECK] 원본 파일 크기 = {orig_size_mb:.1f} MB (TARGET={TARGET_SIZE_MB} MB)")

                raw_local_path: Optional[Path] = None
                enc_local_path: Optional[Path] = None

                # Encoded 필요 여부에 따라 인코딩 또는 복사 결정
                if encoded_required:
                    if orig_size_mb <= TARGET_SIZE_MB:
                        # 인코딩 없이 Encoded 처리
                        if raw_required:
                            # Raw도 필요하므로, Raw는 local_in, Encoded는 복사본 사용
                            raw_local_path = local_in
                            enc_local_path = local_out
                            shutil.copy2(local_in, enc_local_path)
                            print(f"[INFO] 원본이 목표 용량 이하 -> 인코딩 생략, Encoded용 파일 복사 생성: {enc_local_path}")
                        else:
                            # Raw는 이미 GDrive에 존재, Encoded만 필요 → 원본을 Encoded로 사용
                            enc_local_path = local_in
                            print(f"[INFO] 원본이 목표 용량 이하 -> 인코딩 생략, Encoded에 원본 직접 사용")
                    else:
                        # 인코딩 필요
                        raw_local_path = local_in if raw_required else None
                        enc_local_path = local_out
                        print(f"[PROGRESS][{flat_name}] 인코딩 준비 10%")
                        print(f"[STEP] ffmpeg 인코딩 시작")
                        encode_video_to_target_size(local_in, enc_local_path, TARGET_SIZE_MB)
                        print(f"[PROGRESS][{flat_name}] 인코딩 완료 80%")
                else:
                    # Encoded 필요 없음, Raw만 필요한 경우
                    if raw_required:
                        raw_local_path = local_in

                # Raw가 아직 지정 안 된 경우, 필요하다면 local_in 사용
                if raw_required and raw_local_path is None:
                    raw_local_path = local_in

                # 5) 업로드 작업 큐에 추가
                if raw_required and raw_local_path is not None:
                    upload_queue.put((str(raw_local_path), gdrive_raw_parent_id, raw_name, "raw"))
                    print(f"[QUEUE] Raw 업로드 대기열 추가: {raw_name}")

                if encoded_required and enc_local_path is not None:
                    upload_queue.put((str(enc_local_path), gdrive_encoded_parent_id, flat_name, "encoded"))
                    print(f"[QUEUE] Encoded 업로드 대기열 추가: {flat_name}")

                # 여기까지 오면 다운로드/인코딩 단계는 성공으로 간주
                success = True

            except Exception as e:
                last_error = e
                print(f"[ERROR] 시도 {attempts}/{MAX_RETRIES} 중 오류 발생: {path_display}")
                print(f"        {type(e).__name__}: {e}")
                if attempts < MAX_RETRIES:
                    print(f"[RETRY] {flat_name} 다시 시도 예정...")
                else:
                    print(f"[GIVEUP] {flat_name} - 최대 재시도 횟수 초과, 이 파일은 건너뜀.")
                    failed_files += 1

                # 실패 시 임시 파일 정리 (혹시 남아있다면)
                try:
                    local_in.unlink(missing_ok=True)
                except Exception:
                    pass
                try:
                    local_out.unlink(missing_ok=True)
                except Exception:
                    pass

        if success:
            print(f"[PROGRESS][{flat_name}] 다운로드/인코딩 단계 완료 (업로드는 워커에서 진행)")
        else:
            print(f"[FAIL] 최종 실패 파일(다운로드/인코딩 단계): {path_display}")
            if last_error:
                print(f"      마지막 에러: {type(last_error).__name__}: {last_error}")

    # 업로드 큐가 모두 비워질 때까지 대기
    print("\n[WAIT] 모든 업로드 작업 완료 대기 중...")
    upload_queue.join()

    # 워커 종료 신호 보내기
    for _ in workers:
        upload_queue.put(STOP_SENTINEL)
    for t in workers:
        t.join()

    print(f"\n[DONE] 총 대상 파일 수: {total_targets}")
    print(f"[DONE] (폐강)으로 스킵된 파일 수: {skipped_closed}")
    print(f"[DONE] Encoded 이미 GDrive에 있어 스킵된 파일 수(대략): {skipped_existing_encoded}")
    print(f"[DONE] Raw 이미 GDrive에 있어 스킵된 파일 수(대략): {skipped_existing_raw}")
    print(f"[DONE] 다운로드/인코딩 단계에서 최종 실패한 파일 수: {failed_files}")
    if DRY_RUN:
        print("[DONE] DRY_RUN 모드: 실제 다운로드/인코딩/업로드는 수행되지 않았습니다.")


if __name__ == "__main__":
    process_all()
