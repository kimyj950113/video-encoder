from __future__ import annotations

import argparse
import io
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Tuple

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# 읽기 전용(다운로드만)
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
FOLDER_MIMETYPE = "application/vnd.google-apps.folder"
GOOGLE_APP_PREFIX = "application/vnd.google-apps."


def _escape_drive_q(s: str) -> str:
    return s.replace("'", "\\'")


def get_gdrive_service(credentials_path: str = "credentials.json", token_path: str = "token.json"):
    creds = None
    token_p = Path(token_path)

    if token_p.exists():
        creds = Credentials.from_authorized_user_file(str(token_p), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        token_p.write_text(creds.to_json(), encoding="utf-8")

    return build("drive", "v3", credentials=creds)


def get_drive_id(service, root_folder_id: str) -> Optional[str]:
    meta = service.files().get(
        fileId=root_folder_id,
        fields="id,name,driveId",
        supportsAllDrives=True,
    ).execute()
    return meta.get("driveId")


def drive_list(service, *, q: str, fields: str, drive_id: Optional[str], page_token: Optional[str] = None):
    kwargs = dict(
        q=q,
        fields=fields,
        spaces="drive",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        pageSize=1000,
    )
    if drive_id:
        # 공유드라이브면 corpora 기본(user)로 검색 누락될 수 있어 drive로 고정
        kwargs.update(dict(corpora="drive", driveId=drive_id))
    if page_token:
        kwargs["pageToken"] = page_token
    return service.files().list(**kwargs).execute()


def list_children(service, parent_id: str, drive_id: Optional[str]) -> List[dict]:
    q = f"'{parent_id}' in parents and trashed = false"
    fields = "nextPageToken, files(id,name,mimeType,size)"
    out: List[dict] = []
    token = None
    while True:
        res = drive_list(service, q=q, fields=fields, drive_id=drive_id, page_token=token)
        out.extend(res.get("files", []))
        token = res.get("nextPageToken")
        if not token:
            break
    return out


@dataclass
class EncodedFolder:
    folder_id: str
    rel_path: Path  # root 기준 상대경로 (예: 디자인/.../encoded)


def find_all_encoded_folders(service, root_id: str, drive_id: Optional[str]) -> List[EncodedFolder]:
    """
    root 아래 폴더 트리를 BFS로 훑으면서 name == 'encoded' 인 폴더를 모두 수집.
    """
    encoded: List[EncodedFolder] = []
    queue: List[Tuple[str, Path]] = [(root_id, Path())]  # (folder_id, relative_path)

    while queue:
        fid, rel = queue.pop(0)
        children = list_children(service, fid, drive_id)

        for item in children:
            if item.get("mimeType") != FOLDER_MIMETYPE:
                continue
            name = item["name"]
            child_id = item["id"]
            child_rel = rel / name

            if name == "encoded":
                encoded.append(EncodedFolder(folder_id=child_id, rel_path=child_rel))

            queue.append((child_id, child_rel))

    return encoded


def download_file(service, file_id: str, target_path: Path, size_bytes: Optional[int], chunksize: int, skip_existing: bool):
    target_path.parent.mkdir(parents=True, exist_ok=True)

    # 스킵: 동일 파일이 이미 있고 size가 같으면 스킵
    if skip_existing and target_path.exists() and size_bytes is not None:
        if target_path.stat().st_size == size_bytes:
            print(f"[SKIP] exists: {target_path}")
            return

    tmp_path = target_path.with_suffix(target_path.suffix + ".part")
    if tmp_path.exists():
        # 이어받기는 구현 복잡도가 커서(범위 요청/상태 관리), 안전하게 재시작
        tmp_path.unlink()

    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)

    with io.FileIO(str(tmp_path), mode="wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=chunksize)
        done = False
        last_bucket = -1

        while not done:
            status, done = downloader.next_chunk()
            if status and size_bytes:
                pct = int(status.progress() * 100)
                bucket = pct // 10
                if bucket != last_bucket:
                    last_bucket = bucket
                    print(f"  - {target_path.name}: {bucket*10}%")

    tmp_path.replace(target_path)
    print(f"[DONE] {target_path}")


def download_tree_under_encoded(
    service,
    encoded_folder: EncodedFolder,
    drive_id: Optional[str],
    out_root: Path,
    chunksize: int,
    skip_existing: bool,
    only_mp4: bool,
):
    """
    encoded 폴더 아래를 재귀(BFS)로 내려받기. 폴더 구조는 로컬에 그대로 복제.
    """
    queue: List[Tuple[str, Path]] = [(encoded_folder.folder_id, encoded_folder.rel_path)]

    while queue:
        fid, rel = queue.pop(0)
        children = list_children(service, fid, drive_id)

        # 폴더 먼저 큐잉
        for item in children:
            if item.get("mimeType") == FOLDER_MIMETYPE:
                queue.append((item["id"], rel / item["name"]))

        # 파일 다운로드
        for item in children:
            mime = item.get("mimeType", "")
            if mime == FOLDER_MIMETYPE:
                continue

            # 구글 문서류(내보내기 필요)는 제외
            if mime.startswith(GOOGLE_APP_PREFIX):
                print(f"[SKIP] google-apps file (export needed): {rel/item['name']}")
                continue

            name = item["name"]
            if only_mp4 and not name.lower().endswith(".mp4"):
                continue

            size_bytes = int(item.get("size", 0) or 0) or None
            local_path = out_root / rel / name

            download_file(
                service,
                file_id=item["id"],
                target_path=local_path,
                size_bytes=size_bytes,
                chunksize=chunksize,
                skip_existing=skip_existing,
            )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root-id", required=True, help="탐색 시작 폴더 ID(예: 디자인 루트 폴더 ID)")
    ap.add_argument("--out", required=True, help="다운로드 받을 로컬 루트 폴더")
    ap.add_argument("--credentials", default="credentials.json", help="OAuth client secrets JSON")
    ap.add_argument("--token", default="token.json", help="저장될 token.json 경로")
    ap.add_argument("--chunksize-mb", type=int, default=16, help="다운로드 청크 크기(MB)")
    ap.add_argument("--skip-existing", action="store_true", help="로컬에 동일 크기 파일이 있으면 스킵")
    ap.add_argument("--only-mp4", action="store_true", help="mp4만 다운로드")
    args = ap.parse_args()

    out_root = Path(args.out).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    service = get_gdrive_service(credentials_path=args.credentials, token_path=args.token)
    drive_id = get_drive_id(service, args.root_id)
    print(f"[INFO] driveId = {drive_id if drive_id else '(None - My Drive or unknown)'}")

    encoded_folders = find_all_encoded_folders(service, args.root_id, drive_id)
    print(f"[INFO] found encoded folders: {len(encoded_folders)}")
    for ef in encoded_folders:
        print(f"  - {ef.rel_path} (id={ef.folder_id})")

    chunksize = args.chunksize_mb * 1024 * 1024
    for ef in encoded_folders:
        print("\n==============================")
        print(f"[ENCODED] {ef.rel_path}")
        print("==============================")
        download_tree_under_encoded(
            service,
            encoded_folder=ef,
            drive_id=drive_id,
            out_root=out_root,
            chunksize=chunksize,
            skip_existing=args.skip_existing,
            only_mp4=args.only_mp4,
        )

    print("\n[ALL DONE]")


if __name__ == "__main__":
    main()
