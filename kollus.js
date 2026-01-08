import os
import requests

mp4List = [{"title":"박형준 3-2","mp4":"https://bluetiger.zcdn.kollus.com/kr/media-file.mp4?_s=cbc56"}]
    for c in r'<>:"/\|?*':
        name = name.replace(c, '_')
    return name.strip()

for i, item in enumerate(mp4List):
    filename = f"{sanitize(item['title'])}.mp4"
    print(f"[{i+1}/{len(mp4List)}] {filename}")
    
    r = requests.get(item['mp4'], stream=True)
    total = int(r.headers.get('content-length', 0))
    done = 0
    
    with open(f'videos/{filename}', 'wb') as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
            done += len(chunk)
            if total:
                print(f"\r  {done*100/total:.1f}%", end='')
    print(" ✓")

print("완료!")
