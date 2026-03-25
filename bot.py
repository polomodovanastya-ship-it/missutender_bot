"""Telegram-бот: подписка и периодическая рассылка отчёта о тендерах."""
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional

from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import load_config
from database import init_db, add_subscriber, remove_subscriber, get_subscribers, is_subscribed
from scraper import get_daily_digest_data, set_browser_options, set_performance_options, Tender

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Максимум тендеров в списке в одном сообщении (лимит длины сообщения Telegram)
MAX_TENDERS_IN_MESSAGE = 15


def _make_bot(token: str, proxy: Optional[str]):
    """Создаёт Bot; при указании proxy весь трафик к api.telegram.org идёт через него (нужен пакет aiohttp-socks)."""
    proxy = (proxy or "").strip() or None
    if proxy:
        session = AiohttpSession(proxy=proxy)
        return Bot(token=token, session=session)
    return Bot(token=token)


def format_daily_report(
    total: Optional[int],
    matching: List[Tender],
    freshest: List[Tender],
    date_str: str,
    source_status: Dict[str, str],
    days_back: int,
) -> str:
    """Формирует текст отчёта."""
    lines = [
        "📊 Отчёт о тендерах",
        f"Время: {date_str}",
        f"Период анализа: последние {days_back} дн.",
        "",
    ]

    if total is None:
        lines.append("• Всего тендеров на площадке: данные недоступны")
        lines.append("• По ключевым словам и категориям: данные недоступны")
    else:
        lines.append(f"• Всего тендеров на площадке за последние {days_back} дн.: {total}")
        lines.append(
            f"• По ключевым словам и категориям за последние {days_back} дн.: {len(matching)}"
        )
        lines.append("")
        lines.append("5 самых свежих тендеров:")
        if freshest:
            for i, t in enumerate(freshest, 1):
                pub = t.published_at.strftime("%d.%m.%Y") if t.published_at else "дата не указана"
                lines.append(f"{i}. [{pub}] {t.link}")
        else:
            lines.append("Нет тендеров с датой публикации за выбранный период.")

    lines.append("")
    lines.append("Источники:")
    for src, status in source_status.items():
        lines.append(f"- {src}: {status}")
    if matching:
        lines.append("")
        lines.append("Список подходящих тендеров:")
        for i, t in enumerate(matching[:MAX_TENDERS_IN_MESSAGE], 1):
            title = (t.title[:60] + "…") if len(t.title) > 60 else t.title
            lines.append(f"{i}. {title}")
            lines.append(f"   {t.link}")
        if len(matching) > MAX_TENDERS_IN_MESSAGE:
            lines.append(f"… и ещё {len(matching) - MAX_TENDERS_IN_MESSAGE}")
    return "\n".join(lines)


async def run_daily_report(bot: Bot):
    """Сбор данных и рассылка отчёта подписчикам (по расписанию или сразу после старта)."""
    try:
        cfg = load_config()
    except Exception as e:
        logger.error("Ошибка загрузки конфига: %s", e)
        return
    keywords = cfg.get("keywords") or ["ПО", "программное обеспечение"]
    relevant_tag = cfg.get("relevant_tag") or "ПО (программное обеспечение)"
    market_url = cfg.get("market_url") or "https://www.b2b-fix-price.ru/market/"
    extra = cfg.get("extra_sources") or []
    max_crawl = int(cfg.get("max_crawl_pages") or 80)
    days_back = int(cfg.get("analysis_days") or 7)
    browser_enabled = bool(cfg.get("browser_fallback_enabled", True))
    browser_headless = bool(cfg.get("browser_headless", True))
    detail_concurrency = int(cfg.get("detail_fetch_concurrency") or 6)
    set_browser_options(enabled=browser_enabled, headless=browser_headless)
    set_performance_options(detail_fetch_concurrency=detail_concurrency)

    try:
        total, matching, source_status, freshest = await get_daily_digest_data(
            market_url=market_url,
            extra_sources=extra,
            keywords=keywords,
            relevant_tag=relevant_tag,
            max_crawl_pages=max_crawl,
            days_back=days_back,
        )
    except Exception as e:
        logger.exception("Ошибка при сборе тендеров: %s", e)
        return

    subscribers = await get_subscribers()
    if not subscribers:
        logger.info("Нет подписчиков для рассылки")
        return

    date_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    text = format_daily_report(total, matching, freshest, date_str, source_status, days_back)

    for user_id in subscribers:
        try:
            await bot.send_message(user_id, text, disable_web_page_preview=True)
        except Exception as e:
            logger.warning("Не удалось отправить пользователю %s: %s", user_id, e)
    logger.info("Отчёт отправлен: всего %s, по критериям %s", total, len(matching))


async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я бот мониторинга тендерной площадки b2b-fix-price.ru.\n\n"
        "Присылаю отчёт по расписанию: сколько всего тендеров на площадке и сколько "
        "подходят по ключевым словам и категориям (в т.ч. тег «ПО (программное обеспечение)»).\n\n"
        "Команды:\n"
        "/subscribe — подписаться на отчёты\n"
        "/unsubscribe — отписаться\n"
        "/status — проверить статус подписки"
    )


async def cmd_subscribe(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username if message.from_user else None
    await add_subscriber(user_id, username)
    await message.answer("✅ Вы подписаны на отчёты по тендерам.")


async def cmd_unsubscribe(message: Message):
    user_id = message.from_user.id
    await remove_subscriber(user_id)
    await message.answer("Вы отписаны от уведомлений.")


async def cmd_status(message: Message):
    user_id = message.from_user.id
    if await is_subscribed(user_id):
        await message.answer("Вы подписаны на отчёты.")
    else:
        await message.answer("Вы не подписаны. Используйте /subscribe.")


async def main():
    try:
        cfg = load_config()
    except FileNotFoundError as e:
        logger.error("%s", e)
        return
    except Exception as e:
        logger.error("Ошибка конфига: %s", e)
        return

    token = cfg.get("telegram_bot_token")
    if not token:
        logger.error("В config.yaml укажите telegram_bot_token")
        return

    await init_db()

    proxy = cfg.get("telegram_proxy")
    bot = _make_bot(token, proxy if isinstance(proxy, str) else None)
    if proxy:
        logger.info("Запросы к Telegram API через прокси")
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_subscribe, Command("subscribe"))
    dp.message.register(cmd_unsubscribe, Command("unsubscribe"))
    dp.message.register(cmd_status, Command("status"))

    interval_h = max(1, int(cfg.get("report_interval_hours") or 1))

    # Ежедневный отчёт в фиксированное время (например 10:00) — временно отключён.
    # report_time = (cfg.get("daily_report_time") or "10:00").strip()
    # try:
    #     hour, minute = map(int, report_time.split(":"))
    # except Exception:
    #     hour, minute = 10, 0
    # tz_name = cfg.get("timezone") or "Europe/Moscow"
    # try:
    #     from zoneinfo import ZoneInfo
    #     tz = ZoneInfo(tz_name)
    # except Exception:
    #     tz = None
    # scheduler = AsyncIOScheduler(timezone=tz) if tz else AsyncIOScheduler()
    # scheduler.add_job(
    #     run_daily_report,
    #     CronTrigger(hour=hour, minute=minute),
    #     args=(bot,),
    #     id="daily_report",
    # )

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_daily_report,
        "interval",
        hours=interval_h,
        args=(bot,),
        id="hourly_report",
    )
    scheduler.start()
    logger.info("Планировщик: отчёт каждые %s ч + сразу после старта", interval_h)

    asyncio.create_task(_run_report_after_startup(bot))

    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()


async def _run_report_after_startup(bot: Bot):
    """Первый отчёт вскоре после запуска (поллинг уже поднимается параллельно)."""
    await asyncio.sleep(5)
    await run_daily_report(bot)


if __name__ == "__main__":
    asyncio.run(main())
