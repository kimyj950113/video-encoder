from __future__ import annotations

import argparse
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Iterable, Tuple, Dict

from dotenv import load_dotenv
import dropbox
from dropbox.files import FileMetadata

import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google_auth_httplib2 import AuthorizedHttp

load_dotenv()

# Dropbox
DBX_APP_KEY = os.getenv("DBX_APP_KEY")
DBX_APP_SECRET = os.getenv("DBX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")

DEFAULT_DROPBOX_ROOT = "/웹소설 로맨스"
DEFAULT_LOCAL_WORKDIR = "./tmp_work"
DEFAULT_LOCAL_BUCKET = "raw"

# Dropbox download retry
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# Google Drive
SCOPES = ["https://www.googleapis.com/auth/drive"]
GDRIVE_ROOT_FOLDER_ID = "1mMLHBBQn_mmFIHcNuHTpuiViP5JWcHBi"
FOLDER_MIMETYPE = "application/vnd.google-apps.folder"

# GDrive API retry (default)
GDRIVE_MAX_RETRIES = 3
GDRIVE_RETRY_DELAY_SECONDS = 3
GDRIVE_TIMEOUT_SECONDS = 60

# -----------------------
# Helpers
# -----------------------
def _escape_drive_q(s: str) -> str:
    return s.replace("'", "\\'")


def _sleep_with_backoff(base_delay: float, attempt: int, *, cap: float = 60.0) -> None:
    """
    Exponential backoff + jitter.
    attempt: 1..N
    """
    expo = min(cap, base_delay * (2 ** max(0, attempt - 1)))
    jitter = random.uniform(0, 0.25 * expo)
    time.sleep(expo + jitter)


def _is_transient_error(e: Exception) -> bool:
    """
    네트워크/일시 오류로 볼만한 케이스를 넓게 잡음.
    (확실한 분류는 환경별로 다를 수 있어서 fail-open 설계와 함께 사용)
    """
    if isinstance(e, (ConnectionResetError, ConnectionAbortedError, TimeoutError)):
        return True

    # googleapiclient HttpError도 5xx/429면 보통 재시도 가치가 있음
    if isinstance(e, HttpError):
        status = getattr(getattr(e, "resp", None), "status", None)
        if status in (429, 500, 502, 503, 504):
            return True

    msg = str(e).lower()
    transient_markers = [
        "connection reset",
        "connection aborted",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "remote host",
        "10053",
        "10054",
        "tls",
        "ssl",
        "chunkedencodingerror",
    ]
    return any(m in msg for m in transient_markers)


def _gdrive_execute_with_retry(request, *, desc: str = "", max_retries: int, base_delay: int) -> dict:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            return request.execute()
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            last_err = e
            print(f"[GDRIVE-RETRY] {desc} 시도 {attempt}/{max_retries} 실패: {type(e).__name__}: {e}")
            if attempt >= max_retries or not _is_transient_error(e):
                raise
            _sleep_with_backoff(base_delay, attempt)
    raise last_err  # for type checker


# -----------------------
# Dropbox
# -----------------------
def get_dropbox_client() -> dropbox.Dropbox:
    if not DBX_APP_KEY or not DBX_APP_SECRET or not DROPBOX_REFRESH_TOKEN:
        raise RuntimeError("DBX_APP_KEY / DBX_APP_SECRET / DROPBOX_REFRESH_TOKEN 환경변수를 확인하세요.")
    return dropbox.Dropbox(
        app_key=DBX_APP_KEY,
        app_secret=DBX_APP_SECRET,
        oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
    )


def list_dropbox_files_recursive(dbx: dropbox.Dropbox, root_path: str) -> List[FileMetadata]:
    res = dbx.files_list_folder(root_path, recursive=True)
    entries = list(res.entries)
    while res.has_more:
        res = dbx.files_list_folder_continue(res.cursor)
        entries.extend(res.entries)
    return [e for e in entries if isinstance(e, FileMetadata)]


# -----------------------
# Google Drive
# -----------------------
def get_gdrive_service(*, timeout_seconds: int) :
    """
    httplib2 timeout을 명시해서 장시간 hang/불안정 상황을 줄임.
    """
    base_dir = Path(__file__).parent
    token_path = base_dir / "token.json"
    cred_path = base_dir / "credentials.json"

    creds: Optional[Credentials] = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(cred_path), SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    http = httplib2.Http(timeout=timeout_seconds)
    authed_http = AuthorizedHttp(creds, http=http)

    return build("drive", "v3", http=authed_http, cache_discovery=False)


def get_drive_id(service, root_folder_id: str, *, max_retries: int, base_delay: int) -> Optional[str]:
    req = service.files().get(
        fileId=root_folder_id,
        fields="id,name,driveId",
        supportsAllDrives=True,
    )
    meta = _gdrive_execute_with_retry(
        req,
        desc=f"get root meta fileId={root_folder_id}",
        max_retries=max_retries,
        base_delay=base_delay,
    )
    return meta.get("driveId")


def drive_list(service, q: str, fields: str, drive_id: Optional[str], *, max_retries: int, base_delay: int) -> dict:
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
    return _gdrive_execute_with_retry(
        req,
        desc=f"list q={q[:80]}...",
        max_retries=max_retries,
        base_delay=base_delay,
    )


def get_folder_if_exists(
    service,
    parent_id: str,
    name: str,
    drive_id: Optional[str],
    *,
    max_retries: int,
    base_delay: int,
) -> Optional[str]:
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
        max_retries=max_retries,
        base_delay=base_delay,
    )
    files = res.get("files", [])
    if files:
        if len(files) > 1:
            print(f"[WARN] 동일 폴더명 중복 감지: name={name} parent={parent_id} count={len(files)}")
        return files[0]["id"]
    return None


def get_path_if_exists_cached(
    service,
    root_id: str,
    parts: List[str],
    drive_id: Optional[str],
    cache: Dict[Tuple[str, str], Optional[str]],
    *,
    max_retries: int,
    base_delay: int,
) -> Optional[str]:
    current_id = root_id
    for name in parts:
        key = (current_id, name)
        if key in cache:
            folder_id = cache[key]
        else:
            folder_id = get_folder_if_exists(
                service,
                current_id,
                name,
                drive_id,
                max_retries=max_retries,
                base_delay=base_delay,
            )
            cache[key] = folder_id
        if not folder_id:
            return None
        current_id = folder_id
    return current_id


def find_file_in_folder_with_size(
    service,
    parent_id: str,
    filename: str,
    drive_id: Optional[str],
    *,
    max_retries: int,
    base_delay: int,
) -> Optional[Tuple[str, int]]:
    filename_q = _escape_drive_q(filename)
    query = (
        f"name = '{filename_q}' and "
        f"'{parent_id}' in parents and trashed = false"
    )
    res = drive_list(
        service,
        q=query,
        fields="files(id, name, size)",
        drive_id=drive_id,
        max_retries=max_retries,
        base_delay=base_delay,
    )
    files = res.get("files", [])
    if not files:
        return None
    f = files[0]
    size = int(f.get("size", 0) or 0)
    return f["id"], size


def gdrive_has_same_file_strict(
    service,
    *,
    root_id: str,
    drive_id: Optional[str],
    folder_parts: List[str],
    filename: str,
    expected_size_bytes: int,
    folder_cache: Dict[Tuple[str, str], Optional[str]],
    max_retries: int,
    base_delay: int,
) -> Tuple[bool, str]:
    parent_id = get_path_if_exists_cached(
        service,
        root_id,
        folder_parts,
        drive_id,
        folder_cache,
        max_retries=max_retries,
        base_delay=base_delay,
    )
    if not parent_id:
        return False, "folder_missing"

    found = find_file_in_folder_with_size(
        service,
        parent_id,
        filename,
        drive_id,
        max_retries=max_retries,
        base_delay=base_delay,
    )
    if not found:
        return False, "file_missing"

    _, gsize = found
    if gsize <= 0:
        return False, "size_unknown"

    if gsize == expected_size_bytes:
        return True, "same_name_and_size"

    return False, f"size_mismatch(gdrive={gsize},dropbox={expected_size_bytes})"


# -----------------------
# 다운로드/필터
# -----------------------
def should_skip(
    path_display: str,
    *,
    include_substr: Optional[str],
    exclude_substr: Optional[str],
    skip_closed: bool,
    skip_extensions: Iterable[str],
) -> bool:
    if skip_closed and "(폐강" in path_display:
        return True
    if include_substr and include_substr not in path_display:
        return True
    if exclude_substr and exclude_substr in path_display:
        return True

    suffix = Path(path_display).suffix.lower()
    if suffix and suffix in set(x.lower() for x in skip_extensions):
        return True

    return False


def safe_download_to_file(
    dbx: dropbox.Dropbox,
    dbx_path: str,
    out_path: Path,
    *,
    retries: int = MAX_RETRIES,
    base_delay_seconds: int = RETRY_DELAY_SECONDS,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")

    if tmp_path.exists():
        try:
            tmp_path.unlink()
        except Exception:
            pass

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            dbx.files_download_to_file(download_path=str(tmp_path), path=dbx_path)
            tmp_path.replace(out_path)
            return
        except Exception as e:
            last_err = e
            print(f"[DL-RETRY] {dbx_path} 시도 {attempt}/{retries} 실패: {type(e).__name__}: {e}")
            if attempt >= retries or not _is_transient_error(e):
                break
            _sleep_with_backoff(base_delay_seconds, attempt)

    raise RuntimeError(f"Dropbox 다운로드 실패: {dbx_path} ({type(last_err).__name__}: {last_err})")


@dataclass
class Stats:
    total_seen: int = 0
    skipped_filter: int = 0
    skipped_local_exists: int = 0
    skipped_gdrive_same: int = 0
    downloaded: int = 0
    redownloaded: int = 0
    failed: int = 0
    gdrive_check_failed: int = 0


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("--dropbox-root", default=DEFAULT_DROPBOX_ROOT)
    ap.add_argument("--local-workdir", default=DEFAULT_LOCAL_WORKDIR)
    ap.add_argument("--local-bucket", default=DEFAULT_LOCAL_BUCKET)
    ap.add_argument("--dry-run", action="store_true")

    ap.add_argument("--include", default=None)
    ap.add_argument("--exclude", default=None)
    ap.add_argument("--skip-closed", action="store_true")
    ap.add_argument("--skip-ext", action="append", default=[])
    ap.add_argument("--limit", type=int, default=0)

    ap.add_argument("--redownload-if-size-mismatch", action="store_true")

    g = ap.add_mutually_exclusive_group()
    g.add_argument("--check-gdrive", action="store_true")
    g.add_argument("--no-check-gdrive", action="store_true")

    ap.add_argument("--gdrive-root-id", default=GDRIVE_ROOT_FOLDER_ID)

    # GDrive retry/timeout/fail policy
    ap.add_argument("--gdrive-timeout", type=int, default=GDRIVE_TIMEOUT_SECONDS)
    ap.add_argument("--gdrive-max-retries", type=int, default=GDRIVE_MAX_RETRIES)
    ap.add_argument("--gdrive-retry-delay", type=int, default=GDRIVE_RETRY_DELAY_SECONDS)

    # 기본 fail-open: GDrive 확인이 실패해도 다운로드는 진행
    ap.add_argument("--gdrive-fail-closed", action="store_true", help="GDrive 체크 중 오류 발생 시 즉시 중단")

    args = ap.parse_args()

    # 기본은 엄격 체크 ON
    check_gdrive = True
    if args.no_check_gdrive:
        check_gdrive = False
    elif args.check_gdrive:
        check_gdrive = True

    dbx = get_dropbox_client()

    gdrive = None
    drive_id = None
    folder_cache: Dict[Tuple[str, str], Optional[str]] = {}

    if check_gdrive:
        try:
            gdrive = get_gdrive_service(timeout_seconds=args.gdrive_timeout)
            drive_id = get_drive_id(
                gdrive,
                args.gdrive_root_id,
                max_retries=args.gdrive_max_retries,
                base_delay=args.gdrive_retry_delay,
            )
        except Exception as e:
            # 시작 단계에서 GDrive 연결 자체가 불안정할 수 있음
            print(f"[GDRIVE-INIT-WARN] GDrive 초기화 실패: {type(e).__name__}: {e}")
            if args.gdrive_fail_closed:
                raise
            # fail-open: gdrive 체크 비활성화로 전환
            check_gdrive = False
            gdrive = None
            drive_id = None

    base_dir = Path(__file__).parent
    work_dir = (base_dir / args.local_workdir).resolve()
    bucket_dir = work_dir / args.local_bucket
    bucket_dir.mkdir(parents=True, exist_ok=True)

    files = list_dropbox_files_recursive(dbx, args.dropbox_root)
    files = sorted(files, key=lambda m: m.path_display)

    stats = Stats()
    print(f"[INFO] dropbox_root={args.dropbox_root}")
    print(f"[INFO] local_bucket_dir={bucket_dir}")
    print(f"[INFO] entries={len(files)} dry_run={args.dry_run}")
    print(f"[INFO] check_gdrive(strict name+size)={check_gdrive} driveId={drive_id if drive_id else '(None)'}")
    if check_gdrive:
        print(
            f"[INFO] gdrive_timeout={args.gdrive_timeout}s "
            f"gdrive_max_retries={args.gdrive_max_retries} "
            f"gdrive_retry_delay(base)={args.gdrive_retry_delay}s "
            f"fail_closed={args.gdrive_fail_closed}"
        )

    processed = 0
    for meta in files:
        stats.total_seen += 1

        path_display = meta.path_display
        if should_skip(
            path_display,
            include_substr=args.include,
            exclude_substr=args.exclude,
            skip_closed=args.skip_closed,
            skip_extensions=args.skip_ext,
        ):
            stats.skipped_filter += 1
            continue

        rel = path_display.lstrip("/")
        local_path = bucket_dir / rel

        # 1) 로컬 존재 스킵(옵션에 따라 size mismatch면 재다운)
        if local_path.exists():
            if args.redownload_if_size_mismatch:
                local_size = local_path.stat().st_size
                if local_size == meta.size:
                    stats.skipped_local_exists += 1
                    continue
                else:
                    print(f"[LOCAL-MISMATCH] size differ -> redownload: {path_display}")
                    stats.redownloaded += 1
            else:
                stats.skipped_local_exists += 1
                continue

        # 2) GDrive 엄격 스킵(경로+이름+사이즈)
        if check_gdrive and gdrive is not None:
            parts = rel.split("/")
            folder_parts = parts[:-1]
            filename = parts[-1]

            try:
                same, reason = gdrive_has_same_file_strict(
                    gdrive,
                    root_id=args.gdrive_root_id,
                    drive_id=drive_id,
                    folder_parts=folder_parts,
                    filename=filename,
                    expected_size_bytes=int(meta.size),
                    folder_cache=folder_cache,
                    max_retries=args.gdrive_max_retries,
                    base_delay=args.gdrive_retry_delay,
                )
            except Exception as e:
                stats.gdrive_check_failed += 1
                print(f"[GDRIVE-CHECK-WARN] 확인 실패 -> 다운로드 진행: {path_display} ({type(e).__name__}: {e})")
                if args.gdrive_fail_closed:
                    raise
                same, reason = False, f"gdrive_check_error({type(e).__name__})"

            if same:
                stats.skipped_gdrive_same += 1
                print(f"[SKIP-GDRIVE] 이미 존재(동일 size): {path_display} ({reason})")
                continue
            else:
                print(f"[GDRIVE-CHECK] 다운로드 진행: {path_display} ({reason})")

        processed += 1
        if args.limit > 0 and processed > args.limit:
            break

        print("\n==============================")
        print("[LEFTOVER-PRODUCER] 대상 파일")
        print(f"  - Dropbox: {path_display}")
        print(f"  - Local  : {local_path}")
        print(f"  - Size   : {meta.size} bytes")
        print("==============================")

        if args.dry_run:
            continue

        try:
            safe_download_to_file(dbx, meta.path_lower, local_path)
            stats.downloaded += 1
            print(f"[DL] 완료: {local_path}")
        except Exception as e:
            stats.failed += 1
            print(f"[DL-ERROR] 실패: {path_display}")
            print(f"          {type(e).__name__}: {e}")

    print("\n[SUMMARY]")
    print(f"  total_seen          : {stats.total_seen}")
    print(f"  skipped_filter      : {stats.skipped_filter}")
    print(f"  skipped_local_exists: {stats.skipped_local_exists}")
    print(f"  skipped_gdrive_same : {stats.skipped_gdrive_same}")
    print(f"  gdrive_check_failed : {stats.gdrive_check_failed}")
    print(f"  redownloaded        : {stats.redownloaded}")
    print(f"  downloaded          : {stats.downloaded}")
    print(f"  failed              : {stats.failed}")
    print(f"  local_dir           : {bucket_dir}")
    print("  next step           : consumer 업로드 또는 수동 업로드")


if __name__ == "__main__":
    main()
