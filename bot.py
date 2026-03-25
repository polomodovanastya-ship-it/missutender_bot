"""Telegram-бот: подписка и ежедневная рассылка отчёта о тендерах."""
import asyncio
import logging
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import load_config
from database import init_db, add_subscriber, remove_subscriber, get_subscribers, is_subscribed
from scraper import get_daily_digest_data, Tender

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Максимум тендеров в списке в одном сообщении (лимит длины сообщения Telegram)
MAX_TENDERS_IN_MESSAGE = 15


def format_daily_report(total: int, matching: list, date_str: str) -> str:
    """Формирует текст ежедневного отчёта."""
    lines = [
        "📊 Ежедневный отчёт о тендерах",
        f"Дата: {date_str}",
        "",
        f"• Всего тендеров на площадке: {total}",
        f"• По ключевым словам и категориям: {len(matching)}",
    ]
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
    """Один раз в день: сбор данных и рассылка отчёта подписчикам."""
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

    try:
        total, matching = await get_daily_digest_data(
            market_url=market_url,
            extra_sources=extra,
            keywords=keywords,
            relevant_tag=relevant_tag,
            max_crawl_pages=max_crawl,
        )
    except Exception as e:
        logger.exception("Ошибка при сборе тендеров: %s", e)
        return

    subscribers = await get_subscribers()
    if not subscribers:
        logger.info("Нет подписчиков для рассылки")
        return

    date_str = datetime.now().strftime("%d.%m.%Y")
    text = format_daily_report(total, matching, date_str)

    for user_id in subscribers:
        try:
            await bot.send_message(user_id, text, disable_web_page_preview=True)
        except Exception as e:
            logger.warning("Не удалось отправить пользователю %s: %s", user_id, e)
    logger.info("Ежедневный отчёт отправлен: всего %s, по критериям %s", total, len(matching))


async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я бот мониторинга тендерной площадки b2b-fix-price.ru.\n\n"
        "Раз в день присылаю отчёт: сколько всего тендеров на площадке и сколько "
        "подходят по ключевым словам и категориям (в т.ч. тег «ПО (программное обеспечение)»).\n\n"
        "Команды:\n"
        "/subscribe — подписаться на ежедневный отчёт\n"
        "/unsubscribe — отписаться\n"
        "/status — проверить статус подписки"
    )


async def cmd_subscribe(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username if message.from_user else None
    await add_subscriber(user_id, username)
    await message.answer("✅ Вы подписаны на ежедневный отчёт по тендерам.")


async def cmd_unsubscribe(message: Message):
    user_id = message.from_user.id
    await remove_subscriber(user_id)
    await message.answer("Вы отписаны от уведомлений.")


async def cmd_status(message: Message):
    user_id = message.from_user.id
    if await is_subscribed(user_id):
        await message.answer("Вы подписаны на ежедневный отчёт.")
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

    bot = Bot(token=token)
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_subscribe, Command("subscribe"))
    dp.message.register(cmd_unsubscribe, Command("unsubscribe"))
    dp.message.register(cmd_status, Command("status"))

    report_time = (cfg.get("daily_report_time") or "10:00").strip()
    try:
        hour, minute = map(int, report_time.split(":"))
    except Exception:
        hour, minute = 10, 0
    tz_name = cfg.get("timezone") or "Europe/Moscow"
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = None

    scheduler = AsyncIOScheduler(timezone=tz) if tz else AsyncIOScheduler()
    scheduler.add_job(
        run_daily_report,
        CronTrigger(hour=hour, minute=minute),
        args=(bot,),
        id="daily_report",
    )
    scheduler.start()
    logger.info("Планировщик запущен: ежедневный отчёт в %s (%s)", report_time, tz_name)

    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
