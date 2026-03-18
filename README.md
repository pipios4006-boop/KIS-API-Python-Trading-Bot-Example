# 🚀 KIS-API-Python-Trading-Bot-Example

본 프로젝트는 한국투자증권(KIS) Open API를 활용하여 미국 주식 자동매매 시스템을 구축해보는 파이썬(Python) 예제 코드입니다. 이 코드는 증권사 API 통신 방법과 스케줄러 자동화, 텔레그램 봇 제어 등을 학습하기 위한 기술적 레퍼런스로 작성되었습니다.

## 🚨 원작자 저작권 명시 및 게시 중단(Take-down) 정책 필독

👉 본 코드에 구현된 매매 로직(무한매수법)의 모든 아이디어와 저작권, 지적재산권은 원작자인 **'라오어'**님에게 있습니다.  
👉 본 저장소는 순수하게 파이썬과 API를 공부하기 위한 기술적 예제일 뿐이며, 원작자의 공식적인 승인이나 검수를 받은 프로그램이 아닙니다.  
👉 **만약 원작자(라오어님)께서 본 코드의 공유를 원치 않으시거나 삭제를 요청하실 경우, 본 저장소는 어떠한 사전 예고 없이 즉각적으로 삭제(또는 비공개 처리)될 수 있음을 명확히 밝힙니다.**

## ⚠️ 면책 조항 (Disclaimer)

👉 이 코드는 한국투자증권 Open API의 기능과 파이썬 자동화 로직을 학습하기 위해 작성된 교육 및 테스트 목적의 순수 예제 코드입니다.  
👉 특정 투자 전략이나 종목을 추천하거나 투자를 권유하는 목적이 절대 아닙니다.  
👉 본 코드를 실제 투자에 적용하여 발생하는 모든 금전적 손실 및 시스템 오류에 대한 법적, 도의적 책임은 전적으로 코드를 실행한 사용자 본인에게 있습니다.  
👉 본 코드는 어떠한 형태의 수익도 보장하지 않으므로, 반드시 충분한 모의 테스트 후 본인의 책임하에 사용하시기 바랍니다.

## ✨ 주요 기술적 특징 (Key Features)

💎 **Telegram 기반 스마트 UI 제어** : 텔레그램 봇 API를 연동하여 모바일에서도 인라인 버튼을 통해 손쉽게 장부를 조회하고 명령을 내릴 수 있습니다.  
💎 **안전한 KIS API 통신 및 토큰 관리** : OAuth2 기반의 KIS 인증 토큰을 로컬에 안전하게 캐싱하며, 만료 전 자동으로 갱신(Self-Healing)합니다.  
💎 **스마트 장부 동기화 엔진 (TrueSync)** : 미국장 영업일 스케줄(pandas_market_calendars)을 자동으로 인식하여, 장이 열리는 날에만 실제 API 체결 내역을 가져와 가상 장부와 실제 잔고의 오차를 완벽하게 동기화합니다.  
💎 **스케줄러 자동화 (Scheduler)** : 미국 썸머타임(DST)을 자동 감지하여 프리마켓 및 정규장 스케줄을 알아서 조정하고 실행합니다.  
💎 **모듈화된 아키텍처** : 통신(broker.py), 매매 알고리즘(strategy.py), UI(telegram_bot.py), 설정(config.py)이 객체지향적으로 완벽히 분리되어 있어 본인만의 로직으로 커스텀하기 매우 쉽습니다.

## 🛠️ 설치 및 실행 방법 (Installation & Usage)

📌 **1. 필수 환경 (Requirements)**   
✔️ Python 3.9 이상   
✔️ 한국투자증권 Open API 발급 (App Key, App Secret)   
✔️ Telegram Bot Token 및 Chat ID

📌 **2. 패키지 설치**  
pip install requests pandas_market_calendars python-telegram-bot pytz yfinance python-dotenv pillow.  
pip3 install requests.  
pip3 install yfinance.  
pip3 install pytz.  
pip3 install pandas_market_calendars.  
pip3 install "python-telegram-bot[job-queue]"   
 

📌 3. 환경 변수 설정 (.env 파일 생성)
프로젝트 최상단 폴더에 .env 파일을 만들고 아래 양식에 맞게 본인의 키를 입력합니다.

TELEGRAM_TOKEN=나의_텔레그램_봇_토큰  
ADMIN_CHAT_ID=나의_텔레그램_채팅방_ID숫자  
APP_KEY=나의_한국투자증권_APP_KEY  
APP_SECRET=나의_한국투자증권_APP_SECRET  
CANO=나의_계좌번호_앞8자리  
ACNT_PRDT_CD=01 또는 22  

📌 4. 프로그램 실행

python main.py

(권장: 서버 환경에서는 nohup python main.py & 명령어를 사용하여 백그라운드에서 24시간 가동되도록 설정하세요.)

​📂 파일 구조 (Directory Structure)

​📁 main.py: 스케줄러 구동 및 프로그램의 메인 진입점(Entry Point)  
📁 broker.py: 한국투자증권 API 통신 및 데이터 가공을 담당하는 클래스.  
📁 strategy.py: 예산 분배 및 특정 조건에 따른 매수/매도 알고리즘이 구현된 클래스.  
📁 telegram_bot.py / telegram_view.py: 텔레그램 봇 라우터 및 화면(UI) 렌더링을 담당하는 클래스.  
📁 config.py: 각종 JSON 데이터를 저장하고 불러오는 로컬 캐싱/설정 매니저.  
📁 version_history.py: 코드 업데이트 최신(16) 히스토리 기록  
📁 version_archive.py: 코드 업데이트 과거버전(14~15) 히스토리 기록

```bash
