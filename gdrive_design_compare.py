# dropbox_gdrive_design_compare.py
# 목적:
# - Dropbox "/디자인" 아래의 "모든 파일"을 기준으로,
#   Google Drive의 '디자인' 폴더 아래에 동일 상대경로(+파일명) + 동일 size(bytes) 파일이 존재하는지 전수검사
# - 파일 단위 결과 CSV 저장
# - "해당 폴더 하위의 모든 파일이 OK"인 Dropbox 폴더를 삭제 후보로 표시
#   (빈 폴더도 포함 옵션 기본 ON)
# - 삭제 후보는 "최대한 상위 폴더"로 압축하여 txt로 출력
#
# 주요 수정(이번 전달본):
# - 상대경로가 "."로 들어오는 케이스를 루트 ""로 정규화(=> /디자인 이 후보로 '잘못' 뜨는 문제 방지)
# - compress_highest_folders()에서 루트("")가 포함될 때 하위 폴더 압축이 제대로 되도록 보정
# - 출력/리포트에서도 "."를 ""로 정규화하여 일관성 유지
#
# 주의:
# - 파일 OK 판정은 (상대경로 + 파일명 + 파일 크기 bytes) 모두 일치할 때만 OK
# - 구글 문서류(google-apps)는 size가 없거나 export 필요하므로 별도 상태로 분류

from __future__ import annotations

import argparse
import csv
import os
import random
import time
from collections import deque
from dataclasses import dataclass
from pathlib import PurePosixPath, Path
from typing import Dict, List, Optional, Tuple, Set, Iterable, Union

from dotenv import load_dotenv
import dropbox
from dropbox.files import FileMetadata, FolderMetadata

import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google_auth_httplib2 import AuthorizedHttp

load_dotenv()

# =========================
# 환경변수 (Dropbox)
# =========================
DBX_APP_KEY = os.getenv("DBX_APP_KEY")
DBX_APP_SECRET = os.getenv("DBX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")

# =========================
# 기본값
# =========================
DEFAULT_DROPBOX_ROOT = "/디자인"

# Google Drive (읽기 전용)
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
FOLDER_MIMETYPE = "application/vnd.google-apps.folder"
GOOGLE_APP_PREFIX = "application/vnd.google-apps."

# Retry / timeout
GDRIVE_TIMEOUT_SECONDS = 60
GDRIVE_MAX_RETRIES = 3
GDRIVE_RETRY_DELAY_SECONDS = 3


# -----------------------
# Helpers
# -----------------------
def _escape_drive_q(s: str) -> str:
    return s.replace("'", "\\'")


def _sleep_with_backoff(base_delay: float, attempt: int, *, cap: float = 60.0) -> None:
    expo = min(cap, base_delay * (2 ** max(0, attempt - 1)))
    jitter = random.uniform(0, 0.25 * expo)
    time.sleep(expo + jitter)


def _is_transient_error(e: Exception) -> bool:
    if isinstance(e, (ConnectionResetError, ConnectionAbortedError, TimeoutError)):
        return True
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
        "incompleteread",
    ]
    return any(m in msg for m in transient_markers)


def _gdrive_execute_with_retry(request, *, desc: str, max_retries: int, base_delay: int) -> dict:
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
    raise last_err  # type: ignore[misc]


def _norm_rel_folder(folder: str) -> str:
    # Dropbox relative root가 "."로 들어오는 케이스를 루트로 통일
    return "" if folder in ("", ".", "./") else folder


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


DropboxEntry = Union[FileMetadata, FolderMetadata]


def list_dropbox_entries_recursive(dbx: dropbox.Dropbox, root_path: str) -> List[DropboxEntry]:
    """
    파일 + 폴더 전부 수집 (빈 폴더 포함을 위해 FolderMetadata도 유지)
    """
    res = dbx.files_list_folder(root_path, recursive=True)
    entries = list(res.entries)
    while res.has_more:
        res = dbx.files_list_folder_continue(res.cursor)
        entries.extend(res.entries)

    out: List[DropboxEntry] = []
    for e in entries:
        if isinstance(e, (FileMetadata, FolderMetadata)):
            out.append(e)
    return out


# -----------------------
# Google Drive Auth / list
# -----------------------
def get_gdrive_service(*, timeout_seconds: int, credentials_path: str, token_path: str):
    base_dir = Path(__file__).parent
    token_p = (base_dir / token_path).resolve()
    cred_p = (base_dir / credentials_path).resolve()

    creds: Optional[Credentials] = None
    if token_p.exists():
        creds = Credentials.from_authorized_user_file(str(token_p), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # token.json의 scope가 현재 SCOPES와 다르면 여기서 invalid_scope가 날 수 있음
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(cred_p), SCOPES)
            creds = flow.run_local_server(port=0)
        token_p.write_text(creds.to_json(), encoding="utf-8")

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
        req, desc=f"get root meta fileId={root_folder_id}", max_retries=max_retries, base_delay=base_delay
    )
    return meta.get("driveId")


def drive_list_children(
    service,
    parent_id: str,
    drive_id: Optional[str],
    *,
    max_retries: int,
    base_delay: int,
) -> List[dict]:
    q = f"'{parent_id}' in parents and trashed = false"
    fields = "nextPageToken, files(id,name,mimeType,size)"
    items: List[dict] = []
    page_token = None

    while True:
        kwargs = dict(
            q=q,
            fields=fields,
            spaces="drive",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=1000,
            pageToken=page_token,
        )
        if drive_id:
            kwargs.update(dict(corpora="drive", driveId=drive_id))

        req = service.files().list(**kwargs)
        res = _gdrive_execute_with_retry(
            req,
            desc=f"list children parent={parent_id}",
            max_retries=max_retries,
            base_delay=base_delay,
        )
        items.extend(res.get("files", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break

    return items


# -----------------------
# 비교 로직
# -----------------------
@dataclass
class FileMatch:
    rel_path: str              # 디자인 기준 상대경로 (예: "2D/.../a.png")
    dropbox_size: int
    status: str                # OK / MISSING / SIZE_MISMATCH / AMBIGUOUS / GDRIVE_SIZE_UNKNOWN / GDRIVE_GOOGLE_APP / ERROR
    gdrive_size: Optional[int]
    gdrive_file_ids: str       # 매칭 후보들 fileId를 ;로 합쳐서 기록(ambiguous 분석용)
    note: str


@dataclass
class FolderMatch:
    rel_folder: str            # 디자인 기준 상대폴더 (예: "2D/프로젝트A" / ""(루트))
    dropbox_exists: bool
    gdrive_exists: bool
    file_total_under: int
    file_ok_under: int
    deletable: bool
    note: str


def normalize_rel_under_root(any_dropbox_path_display: str, dropbox_root: str) -> str:
    """
    dropbox_path_display: "/디자인/..../file.ext" 또는 "/디자인/.../folder" 또는 "/디자인"
    dropbox_root: "/디자인"
    return: "..../file.ext" 또는 ".../folder" 또는 ""(루트)
    """
    p = PurePosixPath(any_dropbox_path_display)
    root = PurePosixPath(dropbox_root)
    rel = p.relative_to(root).as_posix()
    if rel in ("", ".", "./"):
        return ""
    return rel


def build_gdrive_index_and_folders(
    service,
    gdrive_design_root_id: str,
    *,
    max_retries: int,
    base_delay: int,
) -> Tuple[Dict[str, List[Tuple[str, Optional[int], str]]], Set[str], int]:
    """
    GDrive 디자인 폴더 아래 전수 스캔:
    - file_index: rel_path -> [(fileId, size_bytes_or_None, mimeType)]
    - folder_set: rel_folder ("" 포함) 집합
    """
    drive_id = get_drive_id(service, gdrive_design_root_id, max_retries=max_retries, base_delay=base_delay)

    file_index: Dict[str, List[Tuple[str, Optional[int], str]]] = {}
    folder_set: Set[str] = set([""])  # root
    q = deque([(gdrive_design_root_id, "")])  # (folder_id, rel_prefix)

    scanned_items = 0

    while q:
        folder_id, prefix = q.popleft()
        children = drive_list_children(
            service,
            folder_id,
            drive_id,
            max_retries=max_retries,
            base_delay=base_delay,
        )

        for it in children:
            scanned_items += 1
            name = it.get("name", "")
            mime = it.get("mimeType", "")

            if mime == FOLDER_MIMETYPE:
                next_prefix = f"{prefix}/{name}" if prefix else name
                folder_set.add(_norm_rel_folder(next_prefix))
                q.append((it["id"], next_prefix))
                continue

            rel_path = f"{prefix}/{name}" if prefix else name
            size_val = it.get("size")
            size_bytes: Optional[int] = int(size_val) if size_val is not None else None
            file_index.setdefault(rel_path, []).append((it["id"], size_bytes, mime))

    return file_index, folder_set, scanned_items


def accumulate_folder_counts(
    rel_path: str,
    total: Dict[str, int],
    ok: Dict[str, int],
    is_ok: bool,
) -> None:
    """
    파일 1개에 대해 상위 모든 폴더(상대경로)를 누적 카운트.
    rel_path = "A/B/C/file.ext" -> folders: "", "A", "A/B", "A/B/C"
    """
    parts = rel_path.split("/")
    folders = [""]  # root
    if len(parts) > 1:
        cur: List[str] = []
        for p in parts[:-1]:
            cur.append(p)
            folders.append("/".join(cur))

    for f in folders:
        f = _norm_rel_folder(f)
        total[f] = total.get(f, 0) + 1
        if is_ok:
            ok[f] = ok.get(f, 0) + 1


def compress_highest_folders(full_folders: Set[str]) -> List[str]:
    """
    최대한 상위 폴더로 압축:
    어떤 폴더가 후보면, 그 하위 후보는 제거.
    (루트("")가 후보면, 사실상 전부 삭제 가능이므로 하위는 모두 제거)
    """
    normalized = {_norm_rel_folder(f) for f in full_folders}
    ordered = sorted(normalized, key=lambda x: (x.count("/"), x))

    kept: List[str] = []
    for f in ordered:
        # 이미 kept에 루트("")가 있으면 다른 건 전부 하위로 간주
        if "" in kept and f != "":
            continue

        if any(
            (f == k) or
            (k != "" and f.startswith(k + "/"))
            for k in kept
        ):
            continue
        kept.append(f)

    return kept


# -----------------------
# Main
# -----------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dropbox-root", default=DEFAULT_DROPBOX_ROOT)
    ap.add_argument("--gdrive-design-root-id", required=True, help="구글드라이브 '디자인' 폴더의 folderId")
    ap.add_argument("--credentials", default="credentials.json")
    ap.add_argument("--token", default="token.json")

    ap.add_argument("--out-file-report", default="design_file_migration_audit.csv")
    ap.add_argument("--out-folder-report", default="design_folder_migration_audit.csv")
    ap.add_argument("--out-deletable", default="dropbox_deletable_folders.txt")

    ap.add_argument("--include", default=None)
    ap.add_argument("--exclude", default=None)
    ap.add_argument("--skip-closed", action="store_true")
    ap.add_argument("--skip-ext", action="append", default=[])

    ap.add_argument("--gdrive-timeout", type=int, default=GDRIVE_TIMEOUT_SECONDS)
    ap.add_argument("--gdrive-max-retries", type=int, default=GDRIVE_MAX_RETRIES)
    ap.add_argument("--gdrive-retry-delay", type=int, default=GDRIVE_RETRY_DELAY_SECONDS)

    ap.add_argument("--include-empty-folders", action="store_true", help="빈 폴더(하위 파일 0개)도 삭제 후보에 포함")
    ap.add_argument("--allow-root-delete", action="store_true", help="Dropbox 루트(/디자인) 자체도 후보로 허용(주의)")

    args = ap.parse_args()

    # 기본: 빈 폴더 포함 ON
    if not args.include_empty_folders:
        args.include_empty_folders = True

    # 1) Dropbox 엔트리(파일+폴더) 수집 + 필터
    dbx = get_dropbox_client()
    entries = list_dropbox_entries_recursive(dbx, args.dropbox_root)

    skip_exts = {e.lower() for e in args.skip_ext}

    dropbox_files: List[FileMetadata] = []
    dropbox_folders: Set[str] = set([""])  # root relative folder

    for e in entries:
        p = e.path_display

        # 공통 필터(경로 기반)
        if args.skip_closed and "(폐강" in p:
            continue
        if args.include and args.include not in p:
            continue
        if args.exclude and args.exclude in p:
            continue

        if isinstance(e, FolderMetadata):
            rel_folder = _norm_rel_folder(normalize_rel_under_root(p, args.dropbox_root))
            dropbox_folders.add(rel_folder)

            # 상위 폴더들도 세트에 넣기
            if rel_folder:
                parts = rel_folder.split("/")
                cur: List[str] = []
                for part in parts:
                    cur.append(part)
                    dropbox_folders.add("/".join(cur))
            continue

        if isinstance(e, FileMetadata):
            ext = Path(p).suffix.lower()
            if ext and ext in skip_exts:
                continue

            dropbox_files.append(e)

            # 파일이 속한 폴더 및 상위 폴더들도 Dropbox 폴더로 인식
            rel_file = normalize_rel_under_root(p, args.dropbox_root)
            parent = PurePosixPath(rel_file).parent.as_posix()
            if parent == ".":
                parent = ""
            parent = _norm_rel_folder(parent)

            dropbox_folders.add(parent)

            if parent:
                parts = parent.split("/")
                cur: List[str] = []
                for part in parts:
                    cur.append(part)
                    dropbox_folders.add("/".join(cur))

    dropbox_files = sorted(dropbox_files, key=lambda m: m.path_display)

    print(f"[INFO] dropbox files={len(dropbox_files)} folders(including empty)={len(dropbox_folders)} root={args.dropbox_root}")

    # 2) GDrive 인덱스/폴더세트(한 번만 전수 스캔)
    gdrive = get_gdrive_service(
        timeout_seconds=args.gdrive_timeout,
        credentials_path=args.credentials,
        token_path=args.token,
    )

    print("[INFO] building gdrive index under '디자인' ...")
    gdrive_index, gdrive_folders, scanned_items = build_gdrive_index_and_folders(
        gdrive,
        args.gdrive_design_root_id,
        max_retries=args.gdrive_max_retries,
        base_delay=args.gdrive_retry_delay,
    )
    print(f"[INFO] gdrive scanned items={scanned_items} file_paths={len(gdrive_index)} folders={len(gdrive_folders)}")

    # 3) 파일 비교 + 폴더 누적
    total_by_folder: Dict[str, int] = {f: 0 for f in dropbox_folders}
    ok_by_folder: Dict[str, int] = {f: 0 for f in dropbox_folders}

    file_results: List[FileMatch] = []

    ok_cnt = miss_cnt = mismatch_cnt = amb_cnt = unknown_cnt = gapp_cnt = err_cnt = 0

    for m in dropbox_files:
        try:
            rel = normalize_rel_under_root(m.path_display, args.dropbox_root)
            expected = int(m.size)

            candidates = gdrive_index.get(rel)

            if not candidates:
                file_results.append(FileMatch(rel, expected, "MISSING", None, "", "not_found_in_gdrive_by_relpath"))
                accumulate_folder_counts(rel, total_by_folder, ok_by_folder, is_ok=False)
                miss_cnt += 1
                continue

            if len(candidates) > 1:
                ids = ";".join([c[0] for c in candidates])
                file_results.append(
                    FileMatch(rel, expected, "AMBIGUOUS", None, ids, f"multiple_items_same_relpath(count={len(candidates)})")
                )
                accumulate_folder_counts(rel, total_by_folder, ok_by_folder, is_ok=False)
                amb_cnt += 1
                continue

            file_id, gsize, mime = candidates[0]
            ids = file_id

            if mime.startswith(GOOGLE_APP_PREFIX):
                file_results.append(FileMatch(rel, expected, "GDRIVE_GOOGLE_APP", gsize, ids, f"gdrive_mime={mime}"))
                accumulate_folder_counts(rel, total_by_folder, ok_by_folder, is_ok=False)
                gapp_cnt += 1
                continue

            if gsize is None or gsize <= 0:
                file_results.append(FileMatch(rel, expected, "GDRIVE_SIZE_UNKNOWN", gsize, ids, "gdrive_size_missing"))
                accumulate_folder_counts(rel, total_by_folder, ok_by_folder, is_ok=False)
                unknown_cnt += 1
                continue

            if int(gsize) == expected:
                file_results.append(FileMatch(rel, expected, "OK", gsize, ids, "same_relpath_and_size"))
                accumulate_folder_counts(rel, total_by_folder, ok_by_folder, is_ok=True)
                ok_cnt += 1
            else:
                file_results.append(
                    FileMatch(rel, expected, "SIZE_MISMATCH", gsize, ids, f"size_mismatch(gdrive={gsize},dropbox={expected})")
                )
                accumulate_folder_counts(rel, total_by_folder, ok_by_folder, is_ok=False)
                mismatch_cnt += 1

        except Exception as e:
            rel_fallback = m.path_display
            file_results.append(FileMatch(rel_fallback, int(m.size), "ERROR", None, "", f"{type(e).__name__}: {e}"))
            err_cnt += 1

    # 4) 폴더 삭제 후보 판정 (빈 폴더 포함)
    folder_results: List[FolderMatch] = []
    deletable_folders: Set[str] = set()

    for folder in sorted({_norm_rel_folder(f) for f in dropbox_folders}, key=lambda x: (x.count("/"), x)):
        total = total_by_folder.get(folder, 0)
        ok = ok_by_folder.get(folder, 0)
        g_exists = _norm_rel_folder(folder) in gdrive_folders
        d_exists = True

        if total == 0:
            # 빈 폴더/빈 서브트리
            if args.include_empty_folders:
                # 루트는 기본적으로 위험하니 allow-root-delete 없으면 제외
                if folder == "" and not args.allow_root_delete:
                    deletable = False
                    note = "empty_but_root_delete_not_allowed"
                else:
                    deletable = True
                    note = "empty_under_dropbox (no files under this folder)"
            else:
                deletable = False
                note = "empty_but_excluded_by_flag"
        else:
            # 파일이 있는 폴더: 전부 OK여야 deletable
            if total == ok:
                if folder == "" and not args.allow_root_delete:
                    deletable = False
                    note = "all_ok_but_root_delete_not_allowed"
                else:
                    deletable = True
                    note = "all_files_ok_under_folder"
            else:
                deletable = False
                note = f"not_all_ok(total={total},ok={ok})"

        folder_results.append(
            FolderMatch(
                rel_folder=folder,
                dropbox_exists=d_exists,
                gdrive_exists=g_exists,
                file_total_under=total,
                file_ok_under=ok,
                deletable=deletable,
                note=note if g_exists else (note + " | gdrive_folder_missing"),
            )
        )

        if deletable:
            deletable_folders.add(folder)

    # 5) "최대한 상위"로 압축
    compressed = compress_highest_folders(deletable_folders)

    # 6) 저장
    out_file = Path(args.out_file_report).resolve()
    with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["rel_path_under_design", "dropbox_size", "status", "gdrive_size", "gdrive_file_ids", "note"])
        for r in file_results:
            w.writerow([r.rel_path, r.dropbox_size, r.status, r.gdrive_size, r.gdrive_file_ids, r.note])

    out_folder = Path(args.out_folder_report).resolve()
    with open(out_folder, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["rel_folder_under_design", "dropbox_exists", "gdrive_exists", "file_total_under", "file_ok_under", "deletable", "note"])
        for r in folder_results:
            w.writerow([r.rel_folder, r.dropbox_exists, r.gdrive_exists, r.file_total_under, r.file_ok_under, r.deletable, r.note])

    out_del = Path(args.out_deletable).resolve()
    with open(out_del, "w", encoding="utf-8") as f:
        for folder in compressed:
            folder = _norm_rel_folder(folder)
            abs_path = (PurePosixPath(args.dropbox_root) / folder).as_posix() if folder else args.dropbox_root
            f.write(abs_path + "\n")

    # 7) 요약 출력
    print("\n[SUMMARY - FILE]")
    print(f"  OK               : {ok_cnt}")
    print(f"  MISSING          : {miss_cnt}")
    print(f"  SIZE_MISMATCH    : {mismatch_cnt}")
    print(f"  AMBIGUOUS        : {amb_cnt}")
    print(f"  GDRIVE_SIZE_UNK  : {unknown_cnt}")
    print(f"  GDRIVE_GOOGLEAPP : {gapp_cnt}")
    print(f"  ERROR            : {err_cnt}")
    print(f"  file_report_csv  : {out_file}")

    print("\n[SUMMARY - FOLDER]")
    deletable_cnt = sum(1 for r in folder_results if r.deletable)
    deletable_missing_gdrive = sum(1 for r in folder_results if r.deletable and not r.gdrive_exists)
    print(f"  folders_total(dropbox)         : {len(dropbox_folders)}")
    print(f"  folders_deletable(raw)         : {deletable_cnt}")
    print(f"  folders_deletable_missing_drive: {deletable_missing_gdrive}  (folder_report에서 확인)")
    print(f"  folder_report_csv              : {out_folder}")

    print("\n[DELETE CANDIDATES - DROPBOX] (최대한 상위 폴더)")
    for folder in compressed:
        folder = _norm_rel_folder(folder)
        abs_path = (PurePosixPath(args.dropbox_root) / folder).as_posix() if folder else args.dropbox_root
        print(f"  - {abs_path}")
    print(f"  deletable_list_txt             : {out_del}")

    print("\n[NOTE]")
    print("  - 파일 판정은 '디자인 기준 상대경로 + 파일명 + 파일크기(bytes)'가 모두 일치할 때만 OK 입니다.")
    print("  - 빈 폴더(하위 파일 0개)는 --include-empty-folders(기본 ON)로 삭제 후보에 포함됩니다.")
    print("  - 다만 Drive에 동일 폴더가 없을 수도 있으니, folder_report_csv의 gdrive_exists를 함께 확인하세요.")
    print("  - '/디자인' 루트 삭제 후보는 기본적으로 막혀있고(--allow-root-delete 없으면 False),")
    print("    이번 버전은 '.' 상대폴더를 ''로 정규화하여 '/디자인'이 '.' 때문에 잘못 뜨는 문제를 방지합니다.")


if __name__ == "__main__":
    main()
