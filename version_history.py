VERSION_HISTORY = [
    "V18.0 [2026.03.18] 액면분할 및 병합 대응(Stock Split Wizard) 로직 탑재: /settlement 메뉴 하위에 [✂️ 액면 보정] 추가 및 장부(Ledger) 수량/단가 정밀 소급 조정 (수정: config, telegram_bot)",
    "V18.1 [2026.03.18] API 초당 호출 제한(TPS) 방어(API Throttle) 시스템 구축: 교차 전송(Two-Phase Routing) 및 수동 실행(EXEC) 루프 내부에 0.2초 강제 대기(asyncio.sleep) 적용하여 서버 과부하 차단 (수정: main, telegram_bot)",
    "V18.2 [2026.03.19] KST 자정 넘김 타임 패러독스(Time Paradox) 완벽 차단: 리버스 누적일 캘린더 엔진의 기준 시계를 한국 시간(KST)에서 미국 동부 시간(EST)으로 변경하여 한 세션 내에서 일차가 어긋나는 현상 영구 해결 (수정: config)",
    "V18.3 [2026.03.19] '무한 리버스 1일차' 루프 버그 완벽 해결: 아침 장부 동기화(Sync) 시, 본전(0%) 탈출 목표를 기본값(-15%/-20%)으로 잘못 덮어씌워 리버스가 즉시 해제되는 현상 수정 (수정: telegram_bot)"
]
