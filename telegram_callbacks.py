# ==========================================================
# [telegram_callbacks.py] - 🌟 100% 통합 완성본 🌟 (Full Version)
# MODIFIED: [V28.14 통이관 및 큐 삭제 런타임 에러 완전 소각]
# MODIFIED: [V28.15 그랜드 수술] 물량 통이관(SET_INIT) 콜백 라우터 영구 소각 및 
# V14 <-> V-REV 모드 전환 시 실잔고 '0주 락온(Lock-on)' 절대 방어막 이식 완료
# MODIFIED: [V28.16 UX 팩트 패치] 0주 락온 발동 시 일회성 팝업(Alert) 무반응 맹점을 해체하고 옵션 A 텍스트 박제 완료
# MODIFIED: [V28.18 UX 팩트 패치] 0주 락온 시 자기 자신의 모드(동일 모드) 서브메뉴 진입 허용 렌더링 완료
# MODIFIED: [V28.19 그랜드 수술] KIS API 가짜 0주(Phantom 0-Share) 응답 맹점 원천 차단. 
# holdings None Safe-Casting 쉴드 이식 및 V14 장부 + V-REV 큐 다이렉트 I/O를 결합한 삼중 교차 검증(Triple Verification) 방어막 최종 탑재 완료
# MODIFIED: [V28.22 스냅샷 렌더링 디커플링 수술] 졸업 카드 발급(HIST:IMG) 시 
# 콜백 데이터에 고유 식별자(ID)가 존재할 경우 해당 과거 지층(History)을 
# 100% 정밀 타격하여 렌더링하도록 팩트 라우팅 엔진 이식.
# MODIFIED: [V28.25 그랜드 수술] 동적 수수료율 설정을 위한 INPUT:FEE 콜백 라우팅 분기 신설 완료.
# MODIFIED: [V28.27] 수동 매도로 인한 0주 락온 디커플링 상태 감지 및 /reset 유도 방어막 추가
# MODIFIED: [V28.32] 코파일럿 아키텍처 채택: V14 전용 상방 스나이퍼 로직 충돌 방지를 위한 V-REV 락다운 방어막 원상 복구
# MODIFIED: [V28.33] TQQQ 등 타 종목의 V-REV 횡단 진입 맹점 100% 소각 (SOXL 하드웨어 락온 이식)
# 🚨 [V29.02 UX 팩트 패치] "역사 목록으로 돌아가기(HIST:LIST)" 콜백 시 cmd_history 호출에 따른 런타임 즉사 맹점 소각. 동적 리스트 렌더링 엔진 단독 이식 완료.
# 🚨 [V29.03 핫픽스] UnboundLocalError 런타임 즉사 유발 원흉(AVWAP 내부 로컬 임포트 섀도잉) 100% 소각 완료.
# NEW: [V29.04] queue_ledger.queues 객체 직접 참조 런타임 붕괴 데드코드 전면 소각 및 다이렉트 I/O 멱등성 방어막 이식
# MODIFIED: [V30.09 핫픽스] 잔존 데드코드(pytz) 영구 소각 및 ZoneInfo 이식을 통한 타임존 무결성 락온 통일
# 🚨 MODIFIED: [V32.00] 12차 백테스트 팩트 반영. 불필요해진 AVWAP 동적 파라미터(TARGET_SET, GAP_SET) 콜백 라우팅 전면 소각 완료.
# NEW: [V40.XX 옴니 매트릭스] SOXL/SOXS 듀얼 모멘텀 티커 스위칭 완벽 분기 및 V-REV/AVWAP 권한 개방 완료
# 🚨 MODIFIED: [V42.00 아키텍처 개편] SOXS 메인 종목 모드 전환 영구 락다운 및 듀얼 모멘텀 티커 라우팅 팩트 정립
# 🚨 MODIFIED: [V42.01 갭 스위칭 자율주행] 수동 제어(ON/OFF/THRESH_SET) 콜백 라우터 영구 소각
# 🚨 MODIFIED: [V42.02 핫픽스] 운용 종목 선택 메뉴(/ticker)에서 SOXS 듀얼 콤보 라우팅 원천 차단 (팻핑거 방어)
# 🚨 MODIFIED: [V43.00 작전 통제실 복구] AVWAP 커스텀 목표수익률(Target) 및 근무모드(조기퇴근/출장) 변경 콜백(AVWAP_SET) 라우팅 신설 완료.
# 🚨 MODIFIED: [V43.07] 체력 소진율 연동 익절 자율주행 모드(TARGET_AUTO/MANUAL) 및 리프레시 버튼 콜백 라우터 팩트 추가.
# 🚨 MODIFIED: [V43.11 UI 극한 다이어트] 수동 전환과 목표가 텍스트 입력을 1개 라우터(TARGET_MANUAL)로 통폐합하여 UX 동선 최적화.
# 🚨 MODIFIED: [V43.12 텔레그램 멱등성 붕괴 방어] AVWAP 콘솔 업데이트 시 발생하는 'Message is not modified' 400 에러를 Safe Bypass 쉴드로 원천 차단 완료.
# 🚨 MODIFIED: [V43.13 런타임 캐시 락온] AVWAP_SET 콜백에서 봇 데몬의 팩트 app_data 메모리 참조를 동기화하여 스위칭 증발(Amnesia) 버그 영구 소각 완료.
# 🚨 MODIFIED: [V43.14 직관적 버튼 렌더링] TARGET_MANUAL 콜백이 수동 모드 '전환(토글)'만을 전담하도록 팩트 분리 완료.
# 🚨 MODIFIED: [V43.16 상태 인터셉터 락온] '타점수정' 클릭 시 채팅방 ID 기반 user_states를 정확히 할당하여 텍스트 입력이 무시되던 맹점 완벽 소각 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import os
import json
import time
import math
import asyncio
import yfinance as yf
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

class TelegramCallbacks:
    def __init__(self, config, broker, strategy, queue_ledger, sync_engine, view, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.queue_ledger = queue_ledger
        self.sync_engine = sync_engine
        self.view = view
        self.tx_lock = tx_lock

    def _get_max_holdings_qty(self, ticker, kis_qty):
        v14_qty = 0
        vrev_qty = 0
        
        try:
            ledger = self.cfg.get_ledger()
            net = 0
            for r in ledger:
                if r.get('ticker') == ticker:
                    q = int(float(r.get('qty', 0)))
                    net += q if r.get('side') == 'BUY' else -q
            v14_qty = max(0, net)
        except Exception:
            pass

        try:
            q_file = "data/queue_ledger.json"
            if os.path.exists(q_file):
                with open(q_file, 'r', encoding='utf-8') as f:
                    q_data = json.load(f)
                vrev_qty = sum(int(float(lot.get('qty', 0))) for lot in q_data.get(ticker, []) if int(float(lot.get('qty', 0))) > 0)
        except Exception:
            pass

        return max(kis_qty, v14_qty, vrev_qty)

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller):
        query = update.callback_query
        await query.answer()
        chat_id = update.effective_chat.id
        data = query.data.split(":")
        action, sub = data[0], data[1] if len(data) > 1 else ""

        if action == "UPDATE":
            if sub == "CONFIRM":
                from plugin_updater import SystemUpdater
                updater = SystemUpdater()
                await query.edit_message_text("⏳ <b>[업데이트 승인됨]</b> GitHub 코드를 강제 페칭합니다...", parse_mode='HTML')
                try:
                    success, msg = await updater.pull_latest_code()
                    import html
                    safe_msg = html.escape(msg)
                    if success:
                        await query.edit_message_text(f"✅ <b>[업데이트 완료]</b> {safe_msg}\n\n🔄 데몬을 재가동합니다. 잠시 후 봇이 응답할 것입니다.", parse_mode='HTML')
                        updater.restart_daemon()
                    else:
                        await query.edit_message_text(f"❌ <b>[업데이트 실패]</b>\n▫️ 사유: {safe_msg}", parse_mode='HTML')
                except Exception as e:
                    import html
                    safe_err = html.escape(str(e))
                    await query.edit_message_text(f"🚨 <b>[치명적 오류]</b> 프로세스 예외 발생: {safe_err}", parse_mode='HTML')

            elif sub == "CANCEL":
                await query.edit_message_text("❌ 자가 업데이트를 취소했습니다.", parse_mode='HTML')

        elif action == "QUEUE":
            if sub == "VIEW":
                ticker = data[2]
                if getattr(self, 'queue_ledger', None):
                    q_data = self.queue_ledger.get_queue(ticker)
                else:
                    q_data = []
                    try:
                        if os.path.exists("data/queue_ledger.json"):
                            with open("data/queue_ledger.json", "r", encoding='utf-8') as f:
                                q_data = json.load(f).get(ticker, [])
                    except Exception:
                        pass
                        
                msg, markup = self.view.get_queue_management_menu(ticker, q_data)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "EMERGENCY_REQ":
            ticker = sub
            
            status_code, _ = controller._get_market_status()
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
                
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = QueueLedger()
                
            q_data = self.queue_ledger.get_queue(ticker)
            total_q = sum(item.get("qty", 0) for item in q_data)
            
            if total_q == 0:
                await query.answer("⚠️ 큐(Queue)가 텅 비어있어 수혈할 잔여 물량이 없습니다.", show_alert=True)
                return
            
            emergency_qty = q_data[-1].get('qty', 0)
            emergency_price = q_data[-1].get('price', 0.0)
            
            msg, markup = self.view.get_emergency_moc_confirm_menu(ticker, emergency_qty, emergency_price)
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "EMERGENCY_EXEC":
            ticker = sub
            status_code, _ = controller._get_market_status()
            
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
                
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = QueueLedger()
                
            q_data = self.queue_ledger.get_queue(ticker)
            if not q_data:
                await query.answer("⚠️ 큐(Queue)가 텅 비어있어 수혈할 잔여 물량이 없습니다.", show_alert=True)
                return
                
            await query.answer("⏳ KIS 서버에 수동 긴급 수혈(MOC) 명령을 격발합니다...", show_alert=False)
            
            emergency_qty = q_data[-1].get('qty', 0)
            
            if emergency_qty > 0:
                async with self.tx_lock:
                    res = await asyncio.to_thread(self.broker.send_order, ticker, "SELL", emergency_qty, 0.0, "MOC")
                    
                    if res.get('rt_cd') == '0':
                        self.queue_ledger.pop_lots(ticker, emergency_qty)
                        
                        msg = f"🚨 <b>[{ticker}] 수동 긴급 수혈 (Emergency MOC) 격발 완료!</b>\n"
                        msg += f"▫️ 포트폴리오 매니저의 승인 하에 최근 로트 <b>{emergency_qty}주</b>를 시장가(MOC)로 강제 청산했습니다.\n"
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                        
                        new_q_data = self.queue_ledger.get_queue(ticker)
                        new_msg, markup = self.view.get_queue_management_menu(ticker, new_q_data)
                        await query.edit_message_text(new_msg, reply_markup=markup, parse_mode='HTML')
                    else:
                        err_msg = res.get('msg1', '알 수 없는 에러')
                        await query.edit_message_text(f"❌ <b>[{ticker}] 수동 긴급 수혈 실패:</b> {err_msg}", parse_mode='HTML')

        elif action == "DEL_REQ":
            ticker = sub
            target_date = ":".join(data[2:])
            
            q_data = self.queue_ledger.get_queue(ticker) if getattr(self, 'queue_ledger', None) else []
            if not q_data:
                try:
                    with open("data/queue_ledger.json", "r") as f:
                        q_data = json.load(f).get(ticker, [])
                except Exception:
                    pass
            
            qty, price = 0, 0.0
            for item in q_data:
                if item.get('date') == target_date:
                    qty = item.get('qty', 0)
                    price = item.get('price', 0.0)
                    break
                    
            msg, markup = self.view.get_queue_action_confirm_menu(ticker, target_date, qty, price)
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action in ["DEL_Q", "EDIT_Q"]:
            ticker = sub
            target_date = ":".join(data[2:])
            
            try:
                q_file = "data/queue_ledger.json"
                all_q = {}
                if os.path.exists(q_file):
                    with open(q_file, 'r', encoding='utf-8') as f:
                        all_q = json.load(f)
                
                ticker_q = all_q.get(ticker, [])
                
                if action == "DEL_Q":
                    new_q = [item for item in ticker_q if item.get('date') != target_date]
                    
                    all_q[ticker] = new_q
                    os.makedirs(os.path.dirname(q_file), exist_ok=True)
                    with open(q_file, 'w', encoding='utf-8') as f:
                        json.dump(all_q, f, ensure_ascii=False, indent=4)
                        
                    if getattr(self, 'queue_ledger', None) and hasattr(self.queue_ledger, '_load'):
                        try:
                            self.queue_ledger._load()
                        except:
                            pass
                    
                    await query.answer("✅ 지층 삭제 완료. KIS 원장과 동기화합니다.", show_alert=False)
                    
                    if ticker not in self.sync_engine.sync_locks:
                        self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                    if not self.sync_engine.sync_locks[ticker].locked():
                        await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=True)
                        
                    final_q = self.queue_ledger.get_queue(ticker) if getattr(self, 'queue_ledger', None) else new_q
                    msg, markup = self.view.get_queue_management_menu(ticker, final_q)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    
                elif action == "EDIT_Q":
                    await query.answer("✏️ 수정 모드 진입", show_alert=False)
                    short_date = target_date[:10]
                    controller.user_states[chat_id] = f"EDITQ_{ticker}_{target_date}"
                    
                    prompt = f"✏️ <b>[{ticker} 지층 수정 모드]</b>\n"
                    prompt += f"선택하신 <b>[{short_date}]</b> 지층을 재설정합니다.\n\n"
                    prompt += "새로운 <b>[수량]</b>과 <b>[평단가]</b>를 띄어쓰기로 입력하세요.\n"
                    prompt += "(예: <code>229 52.16</code>)\n\n"
                    prompt += "<i>(입력을 취소하려면 숫자 이외의 문자를 보내주세요)</i>"
                    await query.edit_message_text(prompt, parse_mode='HTML')
            except Exception as e:
                await query.answer(f"❌ 처리 중 에러 발생: {e}", show_alert=True)

        elif action == "VERSION":
            history_data = self.cfg.get_full_version_history()
            if sub == "LATEST":
                msg, markup = self.view.get_version_message(history_data, page_index=None)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "PAGE":
                page_idx = int(data[2])
                msg, markup = self.view.get_version_message(history_data, page_index=page_idx)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                
        elif action == "RESET":
            if sub == "MENU":
                active_tickers = self.cfg.get_active_tickers()
                msg, markup = self.view.get_reset_menu(active_tickers)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "LOCK": 
                ticker = data[2]
                self.cfg.reset_lock_for_ticker(ticker)
                await query.edit_message_text(f"✅ <b>[{ticker}] 금일 매매 잠금이 해제되었습니다.</b>", parse_mode='HTML')
            elif sub == "REV":
                ticker = data[2]
                msg, markup = self.view.get_reset_confirm_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "CONFIRM":
                ticker = data[2]
                
                self.cfg.set_reverse_state(ticker, False, 0)
                self.cfg.clear_escrow_cash(ticker) 
                
                ledger_data = [r for r in self.cfg.get_ledger() if r.get('ticker') != ticker]
                self.cfg._save_json(self.cfg.FILES["LEDGER"], ledger_data)
                
                backup_file = self.cfg.FILES["LEDGER"].replace(".json", "_backup.json")
                if os.path.exists(backup_file):
                    try:
                        with open(backup_file, 'r', encoding='utf-8') as f:
                            b_data = json.load(f)
                        b_data = [r for r in b_data if r.get('ticker') != ticker]
                        with open(backup_file, 'w', encoding='utf-8') as f:
                            json.dump(b_data, f, ensure_ascii=False, indent=4)
                    except Exception:
                        pass
                
                q_file = "data/queue_ledger.json"
                if os.path.exists(q_file):
                    try:
                        with open(q_file, 'r', encoding='utf-8') as f:
                            q_data = json.load(f)
                        if ticker in q_data:
                            del q_data[ticker]
                        with open(q_file, 'w', encoding='utf-8') as f:
                            json.dump(q_data, f, ensure_ascii=False, indent=4)
                    except Exception:
                        pass
                    
                if getattr(self, 'queue_ledger', None) and hasattr(self.queue_ledger, '_load'):
                    try:
                        self.queue_ledger._load()
                    except Exception:
                        pass
                    
                await query.edit_message_text(f"✅ <b>[{ticker}] 삼위일체 소각(Nuke) 및 초기화 완료!</b>\n▫️ 본장부, 백업장부, 큐(Queue), 에스크로의 찌꺼기 데이터가 100% 영구 삭제되었습니다.\n▫️ 다음 매수 진입 시 0주 새출발 디커플링 타점 모드로 완벽히 재시작합니다.", parse_mode='HTML')
            
            elif sub == "CANCEL":
                await query.edit_message_text("❌ 닫았습니다.", parse_mode='HTML')

        elif action == "REC":
            if sub == "VIEW": 
                async with self.tx_lock:
                    _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                await self.sync_engine._display_ledger(data[2], chat_id, context, query=query, pre_fetched_holdings=holdings)
            elif sub == "SYNC": 
                ticker = data[2]
                
                if ticker not in self.sync_engine.sync_locks:
                    self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                    
                if not self.sync_engine.sync_locks[ticker].locked():
                    await query.edit_message_text(f"🔄 <b>[{ticker}] 잔고 기반 대시보드 업데이트 중...</b>", parse_mode='HTML')
                    res = await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=True)
                    if res == "SUCCESS": 
                        async with self.tx_lock:
                            _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                        await self.sync_engine._display_ledger(ticker, chat_id, context, message_obj=query.message, pre_fetched_holdings=holdings)

        elif action == "HIST":
            if sub == "VIEW":
                hid = int(data[2])
                target = next((h for h in self.cfg.get_history() if h['id'] == hid), None)
                if target:
                    safe_trades = target.get('trades', [])
                    for t_rec in safe_trades:
                        if 'ticker' not in t_rec:
                            t_rec['ticker'] = target['ticker']
                        if 'side' not in t_rec:
                            t_rec['side'] = 'BUY'
                            
                    qty, avg, invested, sold = self.cfg.calculate_holdings(target['ticker'], safe_trades)
                    
                    try:
                        msg, markup = self.view.create_ledger_dashboard(target['ticker'], qty, avg, invested, sold, safe_trades, 0, 0, is_history=True, history_id=hid)
                    except TypeError:
                        msg, markup = self.view.create_ledger_dashboard(target['ticker'], qty, avg, invested, sold, safe_trades, 0, 0, is_history=True)
                        
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            
            elif sub == "LIST":
                if hasattr(controller, 'cmd_history'):
                    await controller.cmd_history(update, context)

            elif sub == "IMG":
                ticker = data[2]
                target_id = int(data[3]) if len(data) > 3 else None
                
                hist_list = [h for h in self.cfg.get_history() if h['ticker'] == ticker]
                
                if not hist_list:
                    await context.bot.send_message(chat_id, f"📭 <b>[{ticker}]</b> 발급 가능한 졸업 기록이 존재하지 않습니다.", parse_mode='HTML')
                    return
                
                target_hist = None
                if target_id:
                    target_hist = next((h for h in hist_list if h.get('id') == target_id), None)
                    
                if not target_hist:
                    target_hist = sorted(hist_list, key=lambda x: x.get('end_date', ''), reverse=True)[0]
                
                try:
                    await query.edit_message_text(f"🎨 <b>[{ticker}] 프리미엄 졸업 카드를 렌더링 중입니다...</b>", parse_mode='HTML')
                    img_path = self.view.create_profit_image(
                        ticker=target_hist['ticker'],
                        profit=target_hist['profit'],
                        yield_pct=target_hist['yield'],
                        invested=target_hist['invested'],
                        revenue=target_hist['revenue'],
                        end_date=target_hist['end_date']
                    )
                    if os.path.exists(img_path):
                        with open(img_path, 'rb') as f_out:
                            if img_path.lower().endswith('.gif'):
                                await context.bot.send_animation(chat_id=chat_id, animation=f_out)
                            else:
                                await context.bot.send_photo(chat_id=chat_id, photo=f_out)
                        await query.delete_message()
                    else:
                        await query.edit_message_text("❌ 이미지 생성에 실패했습니다.", parse_mode='HTML')
                except Exception as e:
                    logging.error(f"📸 👑 졸업 이미지 생성/발송 실패: {e}")
                    await query.edit_message_text("❌ 이미지 생성 중 오류가 발생했습니다.", parse_mode='HTML')
            
        elif action == "EXEC":
            t = sub
            ver = self.cfg.get_version(t)
            
            if ver == "V_REV" and getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(t):
                await query.answer("🚨 [격발 차단] 수동(한투 알고리즘) 모드가 가동 중입니다. 지시서를 참고하여 한투 앱(V앱)에서 직접 매매를 걸어주십시오.", show_alert=True)
                return
            
            await query.edit_message_text(f"🚀 {t} 수동 강제 전송 시작 (교차 분리)...")
            
            async with self.tx_lock:
                cash, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                
            if holdings is None:
                return await query.edit_message_text("❌ API 통신 오류로 주문을 실행할 수 없습니다.")
                
            _, allocated_cash = controller._calculate_budget_allocation(cash, self.cfg.get_active_tickers())
            h = holdings.get(t, {'qty':0, 'avg':0})
            
            curr_p = float(await asyncio.to_thread(self.broker.get_current_price, t) or 0.0)
            prev_c = float(await asyncio.to_thread(self.broker.get_previous_close, t) or 0.0)
            safe_avg = float(h.get('avg') or 0.0)
            safe_qty = int(float(h.get('qty') or 0))

            status_code, _ = controller._get_market_status()
            
            if status_code in ["AFTER", "CLOSE", "PRE"]:
                try:
                    def get_yf_close():
                        df = yf.Ticker(t).history(period="5d", interval="1d")
                        return float(df['Close'].iloc[-1]) if not df.empty else None
                    yf_close = await asyncio.wait_for(asyncio.to_thread(get_yf_close), timeout=3.0)
                    if yf_close and yf_close > 0:
                        prev_c = yf_close
                except Exception as e:
                    logging.debug(f"YF 정규장 종가 롤오버 스캔 실패 ({t}): {e}")
                    if curr_p > 0 and prev_c == 0.0:
                        prev_c = curr_p

            if ver == "V_REV":
                if not getattr(self, 'queue_ledger', None):
                    from queue_ledger import QueueLedger
                    self.queue_ledger = QueueLedger()
                    
                q_data = self.queue_ledger.get_queue(t)
                
                cached_snap = self.strategy.v_rev_plugin.load_daily_snapshot(t)
                logic_qty = safe_qty
                if cached_snap and "total_q" in cached_snap:
                    logic_qty = cached_snap["total_q"]

                rev_budget = float(self.cfg.get_seed(t) or 0.0) * 0.15
                half_portion_cash = rev_budget * 0.5
                
                loc_orders = []
                
                if q_data and logic_qty > 0:
                    dates_in_queue = sorted(list(set(item.get('date') for item in q_data if item.get('date'))), reverse=True)
                    l1_qty = 0
                    l1_price = 0.0
                    if dates_in_queue:
                        lots_1 = [item for item in q_data if item.get('date') == dates_in_queue[0]]
                        l1_qty = sum(item.get('qty', 0) for item in lots_1)
                        if l1_qty > 0:
                            l1_price = sum(item.get('qty', 0) * item.get('price', 0.0) for item in lots_1) / l1_qty
                    
                    target_l1 = round(l1_price * 1.006, 2)
                    
                    if l1_qty > 0:
                        loc_orders.append({'side': 'SELL', 'qty': l1_qty, 'price': target_l1, 'type': 'LOC', 'desc': '[1층 단독]'})
                        
                    upper_qty = logic_qty - l1_qty
                    if upper_qty > 0:
                        if safe_avg <= 0.0:
                            msg = f"🚨 <b>[{t}] 수동 장전 차단:</b> KIS API가 유효한 평단가를 반환하지 않았습니다 (avg=0). 주문을 취소합니다."
                            await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                            return

                        upper_invested = (logic_qty * safe_avg) - (l1_qty * l1_price)
                        if upper_invested > 0 and upper_qty > 0:
                            upper_avg = upper_invested / upper_qty
                        else:
                            upper_avg = l1_price
                            
                        target_upper = round(upper_avg * 1.005, 2)
                        loc_orders.append({'side': 'SELL', 'qty': upper_qty, 'price': target_upper, 'type': 'LOC', 'desc': '[상위 재고]'})
                
                if prev_c > 0:
                    b1_price = round(prev_c / 0.935 if logic_qty == 0 else prev_c * 0.995, 2)
                    b2_price = round(prev_c * 0.999 if logic_qty == 0 else prev_c * 0.9725, 2)
                    
                    b1_qty = math.floor(half_portion_cash / b1_price) if b1_price > 0 else 0
                    b2_qty = math.floor(half_portion_cash / b2_price) if b2_price > 0 else 0
                    
                    if b1_qty > 0:
                        loc_orders.append({'side': 'BUY', 'qty': b1_qty, 'price': b1_price, 'type': 'LOC', 'desc': '예방적 매수(Buy1)'})
                    if b2_qty > 0:
                        loc_orders.append({'side': 'BUY', 'qty': b2_qty, 'price': b2_price, 'type': 'LOC', 'desc': '예방적 매수(Buy2)'})
                        
                    if logic_qty == 0:
                        pass 
                    elif b2_qty > 0 and b2_price > 0:
                        for n in range(1, 6):
                            grid_p = round(half_portion_cash / (b2_qty + n), 2)
                            if grid_p >= 0.01 and grid_p < b2_price:
                                loc_orders.append({'side': 'BUY', 'qty': 1, 'price': grid_p, 'type': 'LOC', 'desc': f'예방적 줍줍({n})'})

                msg = f"🛡️ <b>[{t}] V-REV 예방적 양방향 LOC 방어선 수동 장전 완료</b>\n"
                
                if logic_qty == 0:
                    msg += "🚫 <code>[0주 새출발] 기준 평단가 부재로 줍줍 생략 (1층 확보에 예산 100% 집중)</code>\n"
                    
                all_success = True
                for o in loc_orders:
                    res = await asyncio.to_thread(self.broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                    is_success = res.get('rt_cd') == '0'
                    if not is_success:
                        all_success = False
                        
                    err_msg = res.get('msg1', '오류')
                    status_icon = '✅' if is_success else f'❌({err_msg})'
                    msg += f"└ {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                    await asyncio.sleep(0.2)
                    
                if all_success and len(loc_orders) > 0:
                    self.cfg.set_lock(t, "REG")
                    msg += "\n🔒 <b>방어선 전송 완료 (매매 잠금 설정됨)</b>"
                elif len(loc_orders) == 0:
                    msg += "\n⚠️ <b>전송할 방어선(예산/수량)이 없습니다.</b>"
                else:
                    msg += "\n⚠️ <b>일부 방어선 구축 실패 (잠금 보류)</b>"
                    
                await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                return
            
            ma_5day = await asyncio.to_thread(self.broker.get_5day_ma, t)
            
            logic_qty_v14 = safe_qty
            is_manual_vwap = getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(t)
            if is_manual_vwap:
                cached_snap_v14 = self.strategy.v14_vwap_plugin.load_daily_snapshot(t)
                if cached_snap_v14 and "total_q" in cached_snap_v14:
                    logic_qty_v14 = cached_snap_v14["total_q"]

            plan = self.strategy.get_plan(t, curr_p, safe_avg, logic_qty_v14, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], is_simulation=True)
            
            title = f"💎 <b>[{t}] 무매4 정규장 주문 수동 실행</b>\n"
            msg = title
            
            all_success = True
            
            for o in plan.get('core_orders', []):
                res = await asyncio.to_thread(self.broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                if not is_success:
                    all_success = False
                    
                err_msg = res.get('msg1', '오류')
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 1차 필수: {o['desc']} {o['qty']}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
                
            for o in plan.get('bonus_orders', []):
                res = await asyncio.to_thread(self.broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                err_msg = res.get('msg1', '잔금패스')
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
            
            if all_success and len(plan.get('core_orders', [])) > 0:
                self.cfg.set_lock(t, "REG")
                msg += "\n🔒 <b>필수 주문 전송 완료 (잠금 설정됨)</b>"
            else:
                msg += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류)</b>"

            await context.bot.send_message(chat_id, msg, parse_mode='HTML')

        elif action == "SET_VER":
            new_ver = sub
            ticker = data[2]
            current_ver = self.cfg.get_version(ticker)
            
            if ticker == "TQQQ" and new_ver == "V_REV":
                await query.answer("⚠️ [절대 헌법 위반] TQQQ는 V14 무매4 전용 아키텍처입니다. 전환이 차단되었습니다.", show_alert=True)
                return
            if ticker == "SOXS":
                await query.answer("⚠️ [절대 헌법 위반] SOXS는 듀얼 모멘텀 타격용 티커로, 개별 모드 전환이 영구 차단되었습니다.", show_alert=True)
                return

            async with self.tx_lock:
                _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                
            if holdings is None:
                await query.answer("🚨 API 통신 지연으로 잔고를 확인할 수 없어 전환을 차단합니다. 잠시 후 다시 시도해 주세요.", show_alert=True)
                return
                
            kis_qty = int(float(holdings.get(ticker, {}).get('qty', 0)))
            max_qty = self._get_max_holdings_qty(ticker, kis_qty)
            
            if kis_qty == 0 and max_qty > 0 and current_ver != new_ver:
                msg = f"🚨 <b>[ 퀀트 모드 전환 강제 차단: 수동 매도 감지 ]</b>\n\n"
                msg += f"실잔고는 0주이나 장부에 잔여 수량({max_qty}주)이 남아있어 모드 전환이 차단되었습니다.\n"
                msg += "증권사 앱에서 수동으로 전량 매도하셨다면, 채팅창에 <code>/reset</code>을 입력하여 장부를 초기화한 후 다시 시도해주세요."
                await query.edit_message_text(msg, parse_mode='HTML')
                return
            
            if max_qty > 0 and current_ver != new_ver:
                msg = f"🚨 <b>[ 퀀트 모드 전환 강제 차단 ]</b>\n\n"
                msg += f"현재 <b>[{ticker}] {max_qty}주</b>를 보유 중입니다. (삼중 교차 검증)\n"
                msg += "V14 ↔ V-REV 간의 엔진 스위칭은 장부 평단가 오염을 막기 위해 <b>'0주(100% 현금)'</b> 상태에서만 절대적으로 허용됩니다.\n\n"
                msg += "진행 중인 매매 사이클을 전량 익절(0주)로 마무리하신 후 다시 시도해 주십시오."
                await query.edit_message_text(msg, parse_mode='HTML')
                return
            
            if new_ver == "V_REV":
                if not (os.path.exists("strategy_reversion.py") and os.path.exists("queue_ledger.py")):
                    await query.answer("🚨 [개봉박두] V-REV 엔진 모듈 파일이 존재하지 않아 전환할 수 없습니다! (업데이트 필요)", show_alert=True)
                    return
                msg, markup = self.view.get_vrev_mode_selection_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return
            
            elif new_ver == "V14":
                msg, markup = self.view.get_v14_mode_selection_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return
                
            self.cfg.set_version(ticker, new_ver)
            self.cfg.set_upward_sniper_mode(ticker, False)
            if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                self.cfg.set_avwap_hybrid_mode(ticker, False)
            if hasattr(self.cfg, 'set_manual_vwap_mode'):
                self.cfg.set_manual_vwap_mode(ticker, False)
                
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>V14 무매4</b> 모드로 전환되었습니다.\n▫️ /sync 명령어에서 변경된 지시서를 확인하세요.", parse_mode='HTML')

        elif action == "SET_VER_CONFIRM":
            mode_type = sub 
            ticker = data[2]
            current_ver = self.cfg.get_version(ticker)
            
            target_ver = "V_REV" if mode_type in ["AUTO", "MANUAL"] else "V14"

            if ticker == "TQQQ" and target_ver == "V_REV":
                await query.answer("⚠️ [절대 헌법 위반] TQQQ는 V14 무매4 전용 아키텍처입니다. 전환이 차단되었습니다.", show_alert=True)
                return
            if ticker == "SOXS":
                await query.answer("⚠️ [절대 헌법 위반] SOXS는 듀얼 모멘텀 타격용 티커로, 개별 모드 전환이 영구 차단되었습니다.", show_alert=True)
                return

            async with self.tx_lock:
                _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                
            if holdings is None:
                await query.answer("🚨 API 통신 지연으로 잔고를 확인할 수 없어 전환을 차단합니다. 잠시 후 다시 시도해 주세요.", show_alert=True)
                return
                
            kis_qty = int(float(holdings.get(ticker, {}).get('qty', 0)))
            max_qty = self._get_max_holdings_qty(ticker, kis_qty)
            
            if kis_qty == 0 and max_qty > 0 and current_ver != target_ver:
                msg = f"🚨 <b>[ 퀀트 모드 전환 강제 차단: 수동 매도 감지 ]</b>\n\n"
                msg += f"실잔고는 0주이나 장부에 잔여 수량({max_qty}주)이 남아있어 모드 전환이 차단되었습니다.\n"
                msg += "증권사 앱에서 수동으로 전량 매도하셨다면, 채팅창에 <code>/reset</code>을 입력하여 장부를 초기화한 후 다시 시도해주세요."
                await query.edit_message_text(msg, parse_mode='HTML')
                return
            
            if max_qty > 0 and current_ver != target_ver:
                msg = f"🚨 <b>[ 퀀트 모드 전환 강제 차단 ]</b>\n\n"
                msg += f"현재 <b>[{ticker}] {max_qty}주</b>를 보유 중입니다. (삼중 교차 검증)\n"
                msg += "V14 ↔ V-REV 간의 엔진 스위칭은 장부 평단가 오염을 막기 위해 <b>'0주(100% 현금)'</b> 상태에서만 절대적으로 허용됩니다.\n\n"
                msg += "진행 중인 매매 사이클을 전량 익절(0주)로 마무리하신 후 다시 시도해 주십시오."
                await query.edit_message_text(msg, parse_mode='HTML')
                return
            
            if mode_type in ["AUTO", "MANUAL"]:
                self.cfg.set_version(ticker, "V_REV")
                self.cfg.set_upward_sniper_mode(ticker, False)
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    self.cfg.set_avwap_hybrid_mode(ticker, False)
                    
                if mode_type == "MANUAL":
                    self.cfg.set_manual_vwap_mode(ticker, True)
                    mode_txt = "🖐️ 수동 모드 (한투 VWAP 알고리즘 위임)"
                else:
                    self.cfg.set_manual_vwap_mode(ticker, False)
                    mode_txt = "🤖 자동 모드 (자체 VWAP 엔진 정밀타격)"
                    
                await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>V_REV 역추세 하이브리드</b>로 전환되었습니다.\n▫️ <b>운용 방식:</b> {mode_txt}\n▫️ /sync 지시서를 확인해 주십시오.", parse_mode='HTML')
            
            elif mode_type in ["V14_LOC", "V14_VWAP"]:
                self.cfg.set_version(ticker, "V14")
                self.cfg.set_upward_sniper_mode(ticker, False)
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    self.cfg.set_avwap_hybrid_mode(ticker, False)
                    
                if mode_type == "V14_VWAP":
                    self.cfg.set_manual_vwap_mode(ticker, True)
                    mode_txt = "🕒 VWAP 타임 슬라이싱 (유동성 추적)"
                else:
                    self.cfg.set_manual_vwap_mode(ticker, False)
                    mode_txt = "📉 LOC 단일 타격 (초안정성)"
                    
                await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>V14 무매4</b> 모드로 전환되었습니다.\n▫️ <b>집행 방식:</b> {mode_txt}\n▫️ /sync 명령어에서 변경된 지시서를 확인하세요.", parse_mode='HTML')

        elif action == "AVWAP_SET":
            action_type = sub
            ticker = data[2]
            
            app_data = context.bot_data.get('app_data')
            if not app_data:
                try:
                    jobs = context.job_queue.jobs() if context.job_queue else []
                    if jobs and len(jobs) > 0 and jobs[0].data is not None:
                        app_data = jobs[0].data
                except Exception:
                    pass
            if not app_data:
                app_data = {}
                
            tracking_cache = app_data.setdefault('sniper_tracking', {})
            
            if action_type == "TARGET":
                # 🚨 [V43.16] user_states를 정확히 할당하여 텍스트 입력을 인터셉터로 토스
                controller.user_states[chat_id] = f"CONF_AVWAP_TARGET_{ticker}"
                await context.bot.send_message(chat_id, f"✏️ <b>[{ticker}] 수동 목표 수익률(%)</b>을 숫자로 입력하세요.\n(예: 2.0, 3.5, 4.0)\n※ 입력 완료 시 자동으로 '🖐️수동 고정' 모드로 전환됩니다.", parse_mode='HTML')
                await query.answer("목표 수익률 입력 대기 중...", show_alert=False)

            elif action_type == "TARGET_MANUAL":
                tracking_cache[f"AVWAP_TARGET_MODE_{ticker}"] = "MANUAL"
                try:
                    from telegram_avwap_console import AvwapConsolePlugin
                    plugin = AvwapConsolePlugin(self.cfg, self.broker, self.strategy, self.tx_lock)
                    msg, markup = await plugin.get_console_message(app_data)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    await query.answer(f"✅ [{ticker}] 🖐️ 수동 고정 모드로 전환되었습니다.", show_alert=False)
                except Exception as e:
                    if "Message is not modified" in str(e):
                        await query.answer(f"✅ [{ticker}] 이미 🖐️수동 모드입니다.", show_alert=False)
                    else:
                        pass

            elif action_type == "TARGET_AUTO":
                tracking_cache[f"AVWAP_TARGET_MODE_{ticker}"] = "AUTO"
                try:
                    from telegram_avwap_console import AvwapConsolePlugin
                    plugin = AvwapConsolePlugin(self.cfg, self.broker, self.strategy, self.tx_lock)
                    msg, markup = await plugin.get_console_message(app_data)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    await query.answer(f"✅ [{ticker}] 🤖 자율주행 모드로 전환되었습니다.", show_alert=False)
                except Exception as e:
                    if "Message is not modified" in str(e):
                        await query.answer(f"✅ [{ticker}] 이미 최신 상태(🤖자율주행)입니다.", show_alert=False)
                    else:
                        logging.error(f"관제탑 새로고침 에러: {e}")
                        await query.answer("모드 변경 완료. /avwap을 다시 호출해주세요.", show_alert=False)
                    
            elif action_type == "EARLY":
                self.cfg.set_avwap_multi_strike_mode(ticker, False)
                try:
                    from telegram_avwap_console import AvwapConsolePlugin
                    plugin = AvwapConsolePlugin(self.cfg, self.broker, self.strategy, self.tx_lock)
                    msg, markup = await plugin.get_console_message(app_data)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    await query.answer("✅ 조기퇴근 모드(1회 익절)로 전환되었습니다.", show_alert=False)
                except Exception as e:
                    if "Message is not modified" in str(e):
                        await query.answer("✅ 이미 최신 상태(조기퇴근)입니다.", show_alert=False)
                    else:
                        pass
                
            elif action_type == "MULTI":
                self.cfg.set_avwap_multi_strike_mode(ticker, True)
                try:
                    from telegram_avwap_console import AvwapConsolePlugin
                    plugin = AvwapConsolePlugin(self.cfg, self.broker, self.strategy, self.tx_lock)
                    msg, markup = await plugin.get_console_message(app_data)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    await query.answer("✅ 무제한 다중 출장 모드로 전환되었습니다.", show_alert=False)
                except Exception as e:
                    if "Message is not modified" in str(e):
                        await query.answer("✅ 이미 최신 상태(무제한 다중출장)입니다.", show_alert=False)
                    else:
                        pass
                
            elif action_type == "REFRESH":
                try:
                    from telegram_avwap_console import AvwapConsolePlugin
                    plugin = AvwapConsolePlugin(self.cfg, self.broker, self.strategy, self.tx_lock)
                    msg, markup = await plugin.get_console_message(app_data)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    await query.answer("🔄 관제탑 스크린을 최신 팩트로 갱신했습니다.", show_alert=False)
                except Exception as e:
                    if "Message is not modified" in str(e):
                        await query.answer("✅ 시장 변화가 없어 최신 상태가 유지 중입니다.", show_alert=False)
                    else:
                        await query.answer(f"갱신 에러: {e}", show_alert=True)

        elif action == "AVWAP":
            if sub == "MENU":
                ticker = data[2]
                is_hybrid_on = getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False)(ticker)
                if not is_hybrid_on:
                    await query.answer(f"⚠️ [{ticker}] AVWAP 하이브리드 모드가 꺼져있습니다. 먼저 활성화해주세요.", show_alert=True)
                    return
                    
                try:
                    from telegram_avwap_console import AvwapConsolePlugin
                    plugin = AvwapConsolePlugin(self.cfg, self.broker, self.strategy, self.tx_lock)
                    app_data = context.bot_data.get('app_data', {})
                    msg, markup = await plugin.get_console_message(app_data)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "Message is not modified" in str(e):
                        await query.answer("✅ 이미 최신 상태입니다.", show_alert=False)
                    else:
                        await query.edit_message_text(f"❌ 관제탑 호출 에러: {e}", parse_mode='HTML')

        elif action == "MODE":
            mode_val = sub
            ticker = data[2] if len(data) > 2 else "SOXL"
            
            if mode_val == "AVWAP_WARN":
                msg, markup = self.view.get_avwap_warning_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return
            elif mode_val == "AVWAP_ON":
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    self.cfg.set_avwap_hybrid_mode(ticker, True)
                self.cfg.set_upward_sniper_mode(ticker, False) 
                await query.edit_message_text(f"🔥 <b>[{ticker}] 차세대 12차 AVWAP 암살자 모드가 락온(Lock-on) 되었습니다!</b>\n▫️ 남은 가용 예산 100%를 활용하여 장중 딥매수 타점을 정밀 사냥합니다.\n▫️ <code>/avwap</code> 명령어로 독립 관제탑에 접속하세요.", parse_mode='HTML')
                return
            elif mode_val == "AVWAP_OFF":
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    self.cfg.set_avwap_hybrid_mode(ticker, False)
                await query.edit_message_text(f"🛑 <b>[{ticker}] 차세대 AVWAP 하이브리드 전술이 즉시 해제되었습니다.</b>", parse_mode='HTML')
                return

            current_ver = self.cfg.get_version(ticker)
            if current_ver == "V_REV" and mode_val == "ON":
                await query.answer(f"🚨 {current_ver} 모드에서는 로직 충돌 방지를 위해 상방 스나이퍼를 켤 수 없습니다!", show_alert=True)
                return

            self.cfg.set_upward_sniper_mode(ticker, mode_val == "ON")
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 상방 스나이퍼 모드 변경 완료: {'🎯 ON (가동중)' if mode_val == 'ON' else '⚪ OFF (대기중)'}", parse_mode='HTML')
            
        elif action == "TICKER":
            if sub == "ALL":
                target_tickers = ["SOXL", "TQQQ"]
                msg_txt = "SOXL + TQQQ 통합"
            elif "," in sub:
                if "SOXS" in sub.split(","):
                    await query.answer("⚠️ [절대 헌법 위반] SOXS는 듀얼 모멘텀 암살자 전용이므로 메인 장부에 등록할 수 없습니다.", show_alert=True)
                    return
                target_tickers = sub.split(",")
                msg_txt = " + ".join(target_tickers) + " 듀얼 모멘텀"
            else:
                if sub == "SOXS":
                    await query.answer("⚠️ [절대 헌법 위반] SOXS 단독 운용 모드는 영구 폐기되었습니다.", show_alert=True)
                    return
                target_tickers = [sub]
                msg_txt = sub + " 전용"
                
            self.cfg.set_active_tickers(target_tickers)
            await query.edit_message_text(f"✅ <b>[운용 종목 락온 완료]</b>\n▫️ <b>{msg_txt}</b> 모드로 전환되었습니다.\n▫️ /sync를 눌러 확인하십시오.", parse_mode='HTML')
            
        elif action == "SEED":
            ticker = data[2]
            controller.user_states[chat_id] = f"SEED_{sub}_{ticker}"
            await context.bot.send_message(chat_id, f"💵 [{ticker}] 시드머니 금액 입력:", parse_mode='HTML')
            
        elif action == "INPUT":
            ticker = data[2]
            controller.user_states[chat_id] = f"CONF_{sub}_{ticker}"
            
            if sub == "SPLIT":
                ko_name = "분할 횟수"
            elif sub == "TARGET":
                ko_name = "목표 수익률(%)"
            elif sub == "COMPOUND":
                ko_name = "자동 복리율(%)"
            elif sub == "STOCK_SPLIT":
                ko_name = "액면 분할/병합 비율 (예: 10분할은 10, 10병합은 0.1)"
            elif sub == "FEE":
                ko_name = "증권사 수수료율(%)"
            else:
                ko_name = "값"
            
            desc = "숫자만 입력하세요.\n(예: 액면분할 시 1주가 10주가 되었다면 10 입력, 10주가 1주로 병합되었다면 0.1 입력)" if sub == "STOCK_SPLIT" else "숫자만 입력하세요."
            await context.bot.send_message(chat_id, f"✏️ <b>[{ticker}] {ko_name}</b>를 설정합니다.\n{desc}", parse_mode='HTML')
