const collected = [];

const observer = new MutationObserver(() => {
  const iframe = document.querySelector('iframe[src*="kollus.com"]');
  const title = document.querySelector('.modal__desc')?.textContent?.trim() || `video_${collected.length + 1}`;
  
  if (iframe?.src && !collected.some(x => x.url === iframe.src)) {
    collected.push({ title, url: iframe.src });
    console.log(`🎬 ${collected.length}번째: [${title}]`);
  }
});

observer.observe(document.body, { childList: true, subtree: true });
console.log('✅ 모니터링 시작!');

// 전체 워크플로우 (자동 감지 버전)
let idx = 0;
const mp4List = [];
let lastClip = '';

// 버튼 만들기
const btn = document.createElement('button');
btn.innerText = `다음 열기 (0/${collected.length})`;
btn.style = 'position:fixed;top:10px;left:10px;z-index:99999;padding:15px;font-size:16px;background:blue;color:white;cursor:pointer;';
btn.onclick = () => {
  if(collected[idx]) {
    window.open(collected[idx].url, '_blank');
    console.log(`🎬 ${idx}: [${collected[idx].title}]`);
  } else {
    alert('끝!');
  }
};
document.body.appendChild(btn);

// 출력 버튼
const outBtn = document.createElement('button');
outBtn.innerText = '📋 복사';
outBtn.style = 'position:fixed;top:10px;left:200px;z-index:99999;padding:15px;font-size:16px;background:green;color:white;cursor:pointer;';
outBtn.onclick = () => {
  console.log(JSON.stringify(mp4List));
  alert('복사됨! Python에 붙여넣기');
};
document.body.appendChild(outBtn);

// 클립보드 자동 감지 (1초마다)
setInterval(async () => {
  try {
    const clip = await navigator.clipboard.readText();
    if (clip && clip !== lastClip && clip.includes('bluetiger') && clip.includes('.mp4')) {
      lastClip = clip;
      if (idx < collected.length) {
        mp4List.push({ title: collected[idx].title, mp4: clip });
        console.log(`✓ 자동저장: [${collected[idx].title}]`);
        idx++;
        btn.innerText = `다음 열기 (${idx}/${collected.length})`;
      }
    }
  } catch(e) {}
}, 1000);

console.log('✅ 준비완료!');
console.log('1. 파란버튼 → 탭 열림');
console.log('2. 탭에서 북마클릿 클릭 → 자동 저장됨!');
console.log('3. 다 모으면 초록버튼 → 복사');
