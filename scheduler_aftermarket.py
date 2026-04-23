# ==========================================================
# [scheduler_aftermarket.py] - 🌟 100% 분할 캡슐화 완성본 (V30.00) 🌟
# ⚠️ 단일 책임 원칙(SRP) 적용: 애프터마켓 로터리 덫 전담 코어
# 💡 [역할] 16:05 EST (05:05 KST) 잔여 물량 3% 익절 장후 지정가 전송
# 🚨 기존 scheduler_trade.py에서 100% 비파괴적으로 분리 독립 완료
# ==========================================================
import logging
import datetime
import pytz
import asyncio
import traceback
import math

# 🚨 공통 유틸리티 코어 참조
from scheduler_core import is_market_open

# ==========================================================
# 5. 🌙 애프터마켓 로터리 덫 (16:05 EST / 05:05 KST)
# ==========================================================
async def scheduled_after_market_lottery(context):
    if not is_market_open(): return
    
    app_data = context.job.data
    cfg, broker, tx_lock = app_data['cfg'], app_data['broker'], app_data['tx_lock']
    chat_id = context.job.chat_id

    async def _do_lottery():
        async with tx_lock:
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None: return
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}

            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                if version != "V_REV": continue

                is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)
                if is_manual_vwap: continue

                h = safe_holdings.get(t) or {}
                qty = int(float(h.get('qty') or 0))
                avg_price = float(h.get('avg') or 0.0)

                if qty > 0 and avg_price > 0:
                    target_price = math.ceil(avg_price * 1.030 * 100) / 100.0
                    await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                    await asyncio.sleep(0.5)

                    # MODIFIED: [V29.21 핫픽스] 동기 함수 블로킹에 의한 루프 마비 방지 (asyncio.to_thread 래핑)
                    res = await asyncio.to_thread(broker.send_order, t, "SELL", qty, target_price, "AFTER_LIMIT")
                    if res.get('rt_cd') == '0':
                        msg = f"🌙 <b>[{t}] 애프터마켓 3% 로터리 덫(Lottery Trap) 장전 완료</b>\n▫️ 대상 물량: <b>{qty}주</b>\n▫️ 타겟 타겟 가격: <b>${target_price:.2f}</b>"
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                    else:
                        fail_msg = f"❌ <b>[{t}] 애프터마켓 덫 장전 실패:</b> {res.get('msg1', '에러')}"
                        await context.bot.send_message(chat_id=chat_id, text=fail_msg, parse_mode='HTML')

                    await asyncio.sleep(0.2)
    try:
        await asyncio.wait_for(_do_lottery(), timeout=60.0)
    except Exception as e:
        logging.error(f"🚨 애프터마켓 로터리 덫 에러: {e}", exc_info=True)
