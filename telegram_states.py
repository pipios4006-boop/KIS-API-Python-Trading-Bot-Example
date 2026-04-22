# ==========================================================
# [telegram_states.py] - 🌟 100% 통합 완성본 🌟 (Part 1)
# MODIFIED: [V28.13 장부 텍스트 수정 런타임 에러 완전 소각]
# QueueLedger 객체 의존성(AttributeError) 전면 철거. 
# 복잡한 클래스를 거치지 않고 data/queue_ledger.json 파일을 직접 열어 
# 덮어쓰는 순수 다이렉트 파일 I/O(Direct File I/O) 우회망 완벽 이식.
# MODIFIED: [V28.25 동적 수수료율 텍스트 입력 라우터 수술] 
# 사용자가 텔레그램 창에 입력한 수수료(%)를 파싱하여 config에 저장하는 CONF_FEE 상태 처리 로직 완벽 이식.
# NEW: [V28.31] 텔레그램 하단 고정 키보드 텍스트 라우팅 복구 (코파일럿 방식 채택)
# 🚨 [V29.00 NEW] 암살자 조기 퇴근 목표 수익률(AVWAP_TARGET) 텍스트 입력/저장 라우터 개통
# ==========================================================
# NEW: [리팩토링 2단계] 유저 텍스트 입력 및 상태 기계(State Machine) 독립 클래스 분리
import logging
import datetime
import pytz
import os
import json
import asyncio
from telegram import Update
from telegram.ext import ContextTypes

class TelegramStates:
    def __init__(self, config, broker, queue_ledger, sync_engine):
        self.cfg = config
        self.broker = broker
        self.queue_ledger = queue_ledger
        self.sync_engine = sync_engine

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller):
        if not controller._is_admin(update):
            return
            
        chat_id = update.effective_chat.id
        text = update.message.text.strip() if update.message.text else ""
        
        if "통합 지시서" in text or "지시서 조회" in text:
            return await controller.cmd_sync(update, context)
        elif "장부 동기화" in text or "장부 조회" in text:
            return await controller.cmd_record(update, context)
        elif "명예의 전당" in text:
            return await controller.cmd_history(update, context)
        elif "코어 스위칭" in text or "전술 설정" in text or "모드변환" in text or "분할변경" in text:
            return await controller.cmd_settlement(update, context)
        elif "시드머니" in text or "시드 변경" in text or "시드 관리" in text:
            return await controller.cmd_seed(update, context)
        elif "종목 선택" in text:
            return await controller.cmd_ticker(update, context)
        elif "스나이퍼" in text:
            return await controller.cmd_mode(update, context)
        elif "버전" in text or "업데이트 내역" in text:
            return await controller.cmd_version(update, context)
        elif "비상 해제" in text:
            return await controller.cmd_reset(update, context)
        elif "시스템 업데이트" in text or "엔진 업데이트" in text:
            return await controller.cmd_update(update, context)

        state = controller.user_states.get(chat_id)
        
        if not state:
            return

        try:
            if state.startswith("EDITQ_"):
                parts = state.split("_", 2)
                ticker = parts[1]
                target_date = parts[2]
                
                input_parts = text.split()
                if len(input_parts) != 2:
                    del controller.user_states[chat_id]
                    return await update.message.reply_text("❌ 입력 형식 오류입니다. 띄어쓰기로 수량과 평단가를 입력해주세요. (수정 취소됨)")
                
                try:
                    qty = int(input_parts[0])
                    price = float(input_parts[1])
                except ValueError:
                    del controller.user_states[chat_id]
                    return await update.message.reply_text("❌ 수량/평단가는 숫자로 입력하세요. (수정 취소됨)")
                
                try:
                    curr_p = await asyncio.wait_for(
                        asyncio.to_thread(self.broker.get_current_price, ticker), 
                        timeout=3.0
                    )
                    if curr_p and curr_p > 0 and (price < curr_p * 0.7 or price > curr_p * 1.3):
                        del controller.user_states[chat_id]
                        return await update.message.reply_text(f"🚨 <b>팻핑거 방어 가동:</b> 입력가(${price:.2f})가 현재가(${curr_p:.2f}) 대비 ±30%를 초과합니다. 다시 시도해주세요.", parse_mode='HTML')
                except Exception:
                    pass

                q_file = "data/queue_ledger.json"
                all_q = {}
                if os.path.exists(q_file):
                    try:
                        with open(q_file, 'r', encoding='utf-8') as f:
                            all_q = json.load(f)
                    except Exception:
                        pass
                        
                ticker_q = all_q.get(ticker, [])
                for item in ticker_q:
                    if item.get('date') == target_date:
                        item['qty'] = qty
                        item['price'] = price
                        break
                
                all_q[ticker] = ticker_q
                
                os.makedirs(os.path.dirname(q_file), exist_ok=True)
                with open(q_file, 'w', encoding='utf-8') as f:
                    json.dump(all_q, f, ensure_ascii=False, indent=4)
                
                if getattr(self, 'queue_ledger', None) and hasattr(self.queue_ledger, '_load'):
                    try:
                        self.queue_ledger._load()
                    except:
                        pass
                
                del controller.user_states[chat_id]
                short_date = target_date[:10]
                await update.message.reply_text(f"✅ <b>[{ticker}] 지층 정밀 수정 완료! KIS 원장과 동기화합니다.</b>\n▫️ {short_date} | {qty}주 | ${price:.2f}", parse_mode='HTML')
                
                if ticker not in self.sync_engine.sync_locks:
                    self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                if not self.sync_engine.sync_locks[ticker].locked():
                    await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=False)
                    
                return

            # 🚨 [V29.00 NEW] 사용자 조기 퇴근 목표 수익률 텍스트 입력 라우터
            if state.startswith("AVWAP_TARGET_"):
                val = float(text)
                if val <= 0:
                    del controller.user_states[chat_id]
                    return await update.message.reply_text("❌ 오류: 목표 수익률은 0보다 커야 합니다. (입력 취소됨)")
                
                ticker = state.split("_")[2]
                self.cfg.set_avwap_early_target(ticker, val)
                
                msg = f"🏃‍♂️ <b>[{ticker}] 조기 퇴근 목표 수익률 {val}% 락온 완료!</b>\n"
                msg += f"▫️ 다음 스나이핑 적중 시부터 이 목표 수익에 도달하면 장중 언제라도 즉각 전량 익절합니다."
                
                del controller.user_states[chat_id]
                return await update.message.reply_text(msg, parse_mode='HTML')

            val = float(text)
            parts = state.split("_")
            
            if state.startswith("SEED"):
                if val < 0:
                    return await update.message.reply_text("❌ 오류: 시드머니는 0 이상이어야 합니다.")
                    
                action, ticker = parts[1], parts[2]
                curr = self.cfg.get_seed(ticker)
                new_v = curr + val if action == "ADD" else (max(0, curr - val) if action == "SUB" else val)
                self.cfg.set_seed(ticker, new_v)
                await update.message.reply_text(f"✅ [{ticker}] 시드 변경: ${new_v:,.0f}")
                
            elif state.startswith("CONF_SPLIT"):
                if val < 1:
                    return await update.message.reply_text("❌ 오류: 분할 횟수는 1 이상이어야 합니다.")
                    
                ticker = parts[2]
                d = self.cfg._load_json(self.cfg.FILES["SPLIT"], self.cfg.DEFAULT_SPLIT)
                d[ticker] = val
                self.cfg._save_json(self.cfg.FILES["SPLIT"], d)
                await update.message.reply_text(f"✅ [{ticker}] 분할: {int(val)}회")
                
            elif state.startswith("CONF_TARGET"):
                ticker = parts[2]
                d = self.cfg._load_json(self.cfg.FILES["PROFIT_CFG"], self.cfg.DEFAULT_TARGET)
                d[ticker] = val
                self.cfg._save_json(self.cfg.FILES["PROFIT_CFG"], d)
                await update.message.reply_text(f"✅ [{ticker}] 목표 수익률: {val}%")

            elif state.startswith("CONF_COMPOUND"):
                if val < 0:
                    return await update.message.reply_text("❌ 오류: 복리율은 0 이상이어야 합니다.")
                    
                ticker = parts[2]
                self.cfg.set_compound_rate(ticker, val)
                await update.message.reply_text(f"✅ [{ticker}] 졸업 시 자동 복리율: {val}%")

            elif state.startswith("CONF_FEE"):
                if val < 0.0 or val > 10.0:
                    return await update.message.reply_text("🚨 <b>오입력 차단:</b> 수수료율은 0.0% ~ 10.0% 사이여야 합니다.", parse_mode='HTML')
                    
                ticker = parts[2]
                self.cfg.set_fee(ticker, val)
                await update.message.reply_text(f"💳 <b>[{ticker}] 증권사 거래 수수료: {val}% 적용 완료!</b>\n▫️ 다음 명예의 전당 정산부터 수익 연산 시 해당 수수료가 적용됩니다.", parse_mode='HTML')
                
            elif state.startswith("CONF_STOCK_SPLIT"):
                if val <= 0:
                    return await update.message.reply_text("❌ 오류: 액면 보정 비율은 0보다 커야 합니다.")
                    
                ticker = parts[2]
                self.cfg.apply_stock_split(ticker, val)
                
                est = pytz.timezone('US/Eastern')
                today_str = datetime.datetime.now(est).strftime('%Y-%m-%d')
                self.cfg.set_last_split_date(ticker, today_str)
                
                await update.message.reply_text(f"✅ [{ticker}] 수동 액면 보정 완료\n▫️ 모든 장부 기록이 {val}배 비율로 정밀하게 소급 조정되었습니다.")
                
        except ValueError:
            await update.message.reply_text("❌ 오류: 유효한 숫자를 입력하세요. (입력 대기 상태가 강제 해제되었습니다.)")
        except Exception as e:
            await update.message.reply_text(f"❌ 알 수 없는 오류 발생: {str(e)}")
        finally:
            if chat_id in controller.user_states:
                del controller.user_states[chat_id]
