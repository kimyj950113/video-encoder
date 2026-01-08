# video_audit_fix.py
# 목적:
# - Google Drive 내에서 name == "encoded" 폴더를 전수로 찾아서
# - 그 하위(재귀) 파일을 수집하고
# - (기본) 용량이 max_mib 초과인 파일만 CSV에 기록
# - --fix 옵션 시: 초과 파일(스캔 범위 내)을 다운로드 -> 재인코딩 -> 같은 fileId로 update(덮어쓰기)
# - --cleanup 옵션 시: update 성공한 파일에 한해 fix_work의 src/out/part를 정리(삭제)
#
# 주의:
# - SCOPES가 drive.file 이라서 "앱이 접근 권한을 가진 파일"만 조회/수정될 수 있습니다.
#   (공유드라이브/권한 설정에 따라 전수 스캔이 제한될 수 있음)

from __future__ import annotations

import argparse
import csv
import io
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# =========================
# 설정
# =========================
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
GDRIVE_ROOT_FOLDER_ID = "0ALadQkEfuBnGUk9PVA"

LOCAL_WORKDIR = "./tmp_work"
FIX_DIRNAME = "fix_work"

CHUNK_SIZE = 16 * 1024 * 1024  # 16MB

FFMPEG_BIN = "ffmpeg"
FFPROBE_BIN = "ffprobe"

FOLDER_MIMETYPE = "application/vnd.google-apps.folder"
_time_pattern = re.compile(r"time=(\d+):(\d+):(\d+\.\d+)")


# =========================
# 유틸
# =========================
def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


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
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    out = run_cmd(cmd).strip()
    return float(out)


def calc_bitrate_kbps(duration_sec: float, target_bytes: int, safety_margin: float) -> int:
    # target_bytes를 상한으로 삼고 margin으로 더 작게 맞추는 방식
    target_bits = int(target_bytes * safety_margin) * 8
    bps = target_bits / max(duration_sec, 0.001)
    return int(bps / 1000)


def _parse_ffmpeg_time_to_seconds(line: str) -> Optional[float]:
    m = _time_pattern.search(line)
    if not m:
        return None
    h = int(m.group(1))
    mm = int(m.group(2))
    ss = float(m.group(3))
    return h * 3600 + mm * 60 + ss


def encode_design_lecture_profile(
    input_path: Path,
    output_path: Path,
    *,
    target_bytes: int,
    safety_margin: float = 0.93,
) -> None:
    """
    디자인 강의(슬라이드/툴 UI) 가독성 우선 인코딩 프로파일:
    - 2-pass x264
    - scale lanczos
    - tune stillimage
    - 오디오 128k 고정
    - 비트레이트가 너무 낮으면 720p로 다운스케일(텍스트 가독성 목적)
    """
    duration = get_video_duration(input_path)
    total_kbps = calc_bitrate_kbps(duration, target_bytes, safety_margin)

    a_kbps = 128
    v_kbps = max(total_kbps - a_kbps, 300)

    if v_kbps < 1200:
        vf = "scale=-2:720:flags=lanczos"
    else:
        vf = "scale=-2:1080:flags=lanczos"

    print(
        f"[RE-ENCODE] duration={duration:.1f}s target_bytes={target_bytes} margin={safety_margin} "
        f"total~{total_kbps}kbps (v={v_kbps}, a={a_kbps}) vf={vf}"
    )

    passlog = str(output_path) + ".passlog"

    # pass 1 (video only)
    cmd1 = [
        FFMPEG_BIN,
        "-y",
        "-i",
        str(input_path),
        "-vf",
        vf,
        "-c:v",
        "libx264",
        "-preset",
        "slow",
        "-tune",
        "stillimage",
        "-b:v",
        f"{v_kbps}k",
        "-pass",
        "1",
        "-passlogfile",
        passlog,
        "-an",
        "-f",
        "mp4",
        os.devnull,
    ]
    run_cmd(cmd1)

    # pass 2 (video + audio)
    cmd2 = [
        FFMPEG_BIN,
        "-y",
        "-i",
        str(input_path),
        "-vf",
        vf,
        "-c:v",
        "libx264",
        "-preset",
        "slow",
        "-tune",
        "stillimage",
        "-b:v",
        f"{v_kbps}k",
        "-pass",
        "2",
        "-passlogfile",
        passlog,
        "-c:a",
        "aac",
        "-b:a",
        f"{a_kbps}k",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        "-f",
        "mp4",
        str(output_path),
    ]
    run_cmd(cmd2)

    # passlog cleanup
    for ext in ("", ".mbtree"):
        p = Path(passlog + ext)
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass

    size_bytes = output_path.stat().st_size
    print(f"[RE-ENCODE] result = {size_bytes/(1024*1024):.1f} MiB ({size_bytes/1_000_000:.1f} MB_dec)")


# =========================
# GDrive Auth / API
# =========================
def get_gdrive_service():
    base_dir = Path(__file__).parent
    token_path = base_dir / "token.json"
    cred_path = base_dir / "credentials.json"

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(cred_path), SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("drive", "v3", credentials=creds)


def list_children(service, parent_id: str, *, want_folders: Optional[bool] = None) -> List[dict]:
    """
    parent_id의 직계 children을 가져옴.
    want_folders:
      - True: 폴더만
      - False: 폴더 제외(파일만)
      - None: 전부
    """
    q = [f"'{parent_id}' in parents", "trashed = false"]
    if want_folders is True:
        q.append(f"mimeType = '{FOLDER_MIMETYPE}'")
    elif want_folders is False:
        q.append(f"mimeType != '{FOLDER_MIMETYPE}'")
    qstr = " and ".join(q)

    items: List[dict] = []
    page_token = None
    while True:
        res = service.files().list(
            q=qstr,
            fields="nextPageToken, files(id, name, mimeType, size)",
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=1000,
        ).execute()
        items.extend(res.get("files", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return items


def download_file(service, file_id: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)

    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=CHUNK_SIZE)
        done = False
        last_bucket = -1
        while not done:
            status, done = downloader.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                bucket = pct // 10
                if bucket != last_bucket and 0 <= bucket <= 10:
                    last_bucket = bucket
                    print(f"[DL][{file_id}] {bucket*10}%")

    print(f"[DL] done: {out_path}")


def update_file_content(service, file_id: str, local_path: Path) -> None:
    media = MediaFileUpload(str(local_path), resumable=True, chunksize=CHUNK_SIZE)
    req = service.files().update(
        fileId=file_id,
        media_body=media,
        supportsAllDrives=True,
    )

    resp = None
    last_bucket = -1
    print(f"[UPD] start: fileId={file_id} <- {local_path.name}")

    while resp is None:
        status, resp = req.next_chunk()
        if status:
            pct = int(status.progress() * 100)
            bucket = pct // 10
            if bucket != last_bucket and 0 <= bucket <= 10:
                last_bucket = bucket
                print(f"[UPD][{file_id}] {bucket*10}%")

    print(f"[UPD] done: fileId={file_id}")


# =========================
# 스캔 / 보고 / 수정
# =========================
@dataclass
class Row:
    encoded_folder_path: str
    file_name: str
    file_id: str
    size_bytes: int

    @property
    def size_mib(self) -> float:
        return self.size_bytes / (1024 * 1024)

    @property
    def size_mb_dec(self) -> float:
        return self.size_bytes / 1_000_000


def find_all_encoded_folders(service, root_id: str) -> List[Tuple[str, str]]:
    """
    root_id 아래 폴더를 전수 순회하면서 name == 'encoded' 폴더를 수집.
    반환: [(folder_id, folder_path_string)]
    """
    encoded: List[Tuple[str, str]] = []
    stack: List[Tuple[str, List[str]]] = [(root_id, [])]

    while stack:
        folder_id, path_parts = stack.pop()
        child_folders = list_children(service, folder_id, want_folders=True)

        for f in child_folders:
            name = f.get("name", "")
            fid = f["id"]
            new_parts = path_parts + [name]

            if name == "encoded":
                encoded.append((fid, "/" + "/".join(new_parts)))
            else:
                stack.append((fid, new_parts))

    return encoded


def list_all_files_recursive(service, encoded_folder_id: str, encoded_folder_path: str) -> List[Row]:
    """
    encoded_folder_id 아래를 재귀로 내려가며 파일을 수집.
    """
    rows: List[Row] = []
    stack: List[str] = [encoded_folder_id]

    while stack:
        cur = stack.pop()
        children = list_children(service, cur, want_folders=None)

        for item in children:
            if item.get("mimeType") == FOLDER_MIMETYPE:
                stack.append(item["id"])
                continue

            size_str = item.get("size")
            if size_str is None:
                continue

            rows.append(
                Row(
                    encoded_folder_path=encoded_folder_path,
                    file_name=item.get("name", ""),
                    file_id=item["id"],
                    size_bytes=int(size_str),
                )
            )

    return rows


def _safe_unlink(p: Path) -> None:
    if not p.exists():
        return
    try:
        p.unlink()
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fix", action="store_true", help="초과 파일을 다운로드→재인코딩→같은 fileId로 update")
    ap.add_argument("--cleanup", action="store_true", help="update 성공한 파일의 fix_work 산출물(src/out/part) 정리(삭제)")
    ap.add_argument("--max-mib", type=int, default=500, help="최대 용량(MiB, 1MiB=1024*1024 bytes)")
    ap.add_argument("--scan-min-mib", type=float, default=500.0, help="재인코딩 후보 스캔 최소(MiB)")
    ap.add_argument("--scan-max-mib", type=float, default=550.0, help="재인코딩 후보 스캔 최대(MiB)")
    ap.add_argument("--limit-fix", type=int, default=0, help="재인코딩/업데이트 최대 개수(0=무제한)")
    ap.add_argument(
        "--report",
        choices=["oversize", "all"],
        default="oversize",
        help="oversize=초과만 CSV 기록, all=전부 기록",
    )
    args = ap.parse_args()

    service = get_gdrive_service()

    threshold_bytes = args.max_mib * 1024 * 1024
    scan_min_bytes = int(args.scan_min_mib * 1024 * 1024)
    scan_max_bytes = int(args.scan_max_mib * 1024 * 1024)

    base_dir = Path(__file__).parent
    work_dir = (base_dir / LOCAL_WORKDIR).resolve()
    fix_dir = work_dir / FIX_DIRNAME
    fix_dir.mkdir(parents=True, exist_ok=True)

    encoded_folders = find_all_encoded_folders(service, GDRIVE_ROOT_FOLDER_ID)
    print(f"[SCAN] encoded folders found: {len(encoded_folders)}")

    all_rows: List[Row] = []
    for folder_id, folder_path in encoded_folders:
        all_rows.extend(list_all_files_recursive(service, folder_id, folder_path))

    print(f"[SCAN] files under encoded folders: {len(all_rows)}")

    oversize = [r for r in all_rows if r.size_bytes > threshold_bytes]
    oversize_in_range = [r for r in oversize if scan_min_bytes <= r.size_bytes <= scan_max_bytes]

    report_path = base_dir / f"gdrive_encoded_report_{_now_tag()}.csv"
    with open(report_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["encoded_folder", "file_name", "file_id", "size_bytes", "size_MiB", "size_MB_dec"])
        out_rows = all_rows if args.report == "all" else oversize
        for r in out_rows:
            w.writerow(
                [
                    r.encoded_folder_path,
                    r.file_name,
                    r.file_id,
                    r.size_bytes,
                    f"{r.size_mib:.1f}",
                    f"{r.size_mb_dec:.1f}",
                ]
            )

    print(f"[REPORT] saved: {report_path}")
    print(f"[SUMMARY] oversize(>{args.max_mib}MiB): {len(oversize)}")
    print(f"[SUMMARY] oversize in scan-range({args.scan_min_mib}~{args.scan_max_mib}MiB): {len(oversize_in_range)}")

    if not args.fix:
        return

    fixed = 0
    for r in oversize_in_range:
        if args.limit_fix > 0 and fixed >= args.limit_fix:
            print("[FIX] limit reached")
            break

        # 작업 파일 경로(충돌 방지: fileId 기반)
        src_part = fix_dir / f"{r.file_id}.src.part"
        src = fix_dir / f"{r.file_id}.src"
        out_part = fix_dir / f"{r.file_id}.out.mp4.part"
        out = fix_dir / f"{r.file_id}.out.mp4"

        try:
            print("\n==============================")
            print(f"[FIX] {r.encoded_folder_path}/{r.file_name}")
            print(f"[FIX] fileId={r.file_id} size={r.size_mib:.1f}MiB ({r.size_mb_dec:.1f}MB_dec) -> <= {args.max_mib}MiB")
            print("==============================")

            # 시작 시 찌꺼기 정리(이전 실패 잔재)
            for p in (src_part, out_part):
                _safe_unlink(p)
            # src/out은 “재시도”를 위해 남길 수도 있지만, 현재 로직은 항상 새로 받게끔 초기화
            for p in (src, out):
                _safe_unlink(p)

            # 1) 다운로드
            download_file(service, r.file_id, src_part)
            src_part.replace(src)

            # 2) 재인코딩 (margin 2단계로 안정적으로 상한 맞추기)
            margins = [0.93, 0.90]
            ok = False
            last_size: Optional[int] = None

            for m in margins:
                _safe_unlink(out_part)
                encode_design_lecture_profile(src, out_part, target_bytes=threshold_bytes, safety_margin=m)

                # out_part -> out 교체(이전 out 있으면 replace가 덮어씀)
                out_part.replace(out)
                last_size = out.stat().st_size

                if last_size <= threshold_bytes:
                    ok = True
                    break

                print(
                    f"[FIX-WARN] still oversize: {last_size/(1024*1024):.1f}MiB "
                    f"-> retry with lower margin"
                )

            if not ok:
                raise RuntimeError(f"re-encode result still oversize: {last_size} bytes")

            # 3) 같은 fileId로 업데이트(덮어쓰기)
            update_file_content(service, r.file_id, out)

            fixed += 1
            print(f"[FIX-DONE] fileId={r.file_id} new={last_size/(1024*1024):.1f}MiB")

            # 4) cleanup(성공한 경우에만)
            if args.cleanup:
                # 혹시 남아 있을 수 있는 part도 같이 제거
                for p in (src_part, out_part):
                    _safe_unlink(p)
                for p in (src, out):
                    _safe_unlink(p)
                print(f"[CLEANUP] cleaned local artifacts for fileId={r.file_id}")

        except Exception as e:
            print(f"[FIX-ERROR] fileId={r.file_id} {type(e).__name__}: {e}")
            # 실패 시에는 src/out을 남겨서 원인 분석/재시도 가능하게 둠

    print(f"[FIX-SUMMARY] fixed={fixed}")


if __name__ == "__main__":
    main()
