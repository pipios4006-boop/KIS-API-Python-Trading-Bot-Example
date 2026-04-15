# ==========================================================
# [plugin_updater.py]
# ⚠️ 자가 업데이트 및 GCP 데몬 제어 전용 플러그인
# 💡 깃허브 원격 저장소 강제 동기화 (git fetch & reset --hard)
# 💡 OS 레벨 데몬 재가동 제어 (sudo systemctl restart)
# 🚨 [V27.00 핫픽스] 사용자별 데몬 이름(DAEMON_NAME) .env 동적 로드 이식 완료
# ==========================================================
import logging
import asyncio
import subprocess
import os
from dotenv import load_dotenv

class SystemUpdater:
    def __init__(self):
        self.remote_branch = "origin/main"
        
        # 💡 [핵심 수술] .env 파일에서 사용자가 지정한 데몬 이름을 스캔, 없으면 'pipiosbot'으로 폴백
        load_dotenv()
        self.daemon_name = os.getenv("DAEMON_NAME", "pipiosbot")

    async def pull_latest_code(self):
        """
        깃허브 서버와 통신하여 로컬의 변경 사항을 완벽히 무시하고
        원격 저장소의 최신 코드로 강제 덮어쓰기(Hard Reset)를 수행합니다.
        """
        try:
            fetch_proc = await asyncio.create_subprocess_shell(
                "git fetch --all",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            _, fetch_err = await fetch_proc.communicate()
            
            if fetch_proc.returncode != 0:
                error_msg = fetch_err.decode('utf-8').strip()
                logging.error(f"🚨 [Updater] Git Fetch 실패: {error_msg}")
                return False, f"Git Fetch 실패: {error_msg} (서버에서 git init 및 remote add 명령을 선행하십시오)"

            reset_proc = await asyncio.create_subprocess_shell(
                f"git reset --hard {self.remote_branch}",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            _, reset_err = await reset_proc.communicate()
            
            if reset_proc.returncode != 0:
                error_msg = reset_err.decode('utf-8').strip()
                logging.error(f"🚨 [Updater] Git Reset 실패: {error_msg}")
                return False, f"Git Reset 실패: {error_msg}"

            logging.info("✅ [Updater] 깃허브 최신 코드 강제 동기화 완료")
            return True, "깃허브 최신 코드가 로컬에 완벽히 동기화되었습니다."
            
        except Exception as e:
            logging.error(f"🚨 [Updater] 동기화 중 치명적 예외 발생: {e}")
            return False, f"업데이트 프로세스 예외 발생: {e}"

    def restart_daemon(self):
        """
        GCP 리눅스 OS에 데몬 재가동 명령을 하달합니다.
        격발 즉시 봇 프로세스가 SIGTERM 신호를 받고 종료되므로,
        반드시 텔레그램 보고 메시지를 선행 발송한 후 호출해야 합니다.
        """
        try:
            logging.info(f"🔄 [Updater] OS 쉘에 {self.daemon_name} 데몬 재가동 명령을 하달합니다.")
            
            subprocess.Popen(
                ["sudo", "systemctl", "restart", self.daemon_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            return True
        except Exception as e:
            logging.error(f"🚨 [Updater] 데몬 재가동 명령 하달 실패: {e}")
            return False
