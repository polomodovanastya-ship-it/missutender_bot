"""Парсер тендерной площадки b2b-fix-price.ru и дополнительных источников.
Обходит вложенные страницы (категории) и собирает ссылки на страницы тендеров .../tender-<id>/.
"""
import re
import logging
import aiohttp
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List, Optional, Sequence
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

# Шаблон URL страницы тендера: /market/.../tender-4342223/ или /tender-4342223
TENDER_PATH_RE = re.compile(r"/tender-(\d+)/?", re.I)

# Домен площадки для проверки ссылок
MARKET_DOMAIN = "b2b-fix-price.ru"


@dataclass
class Tender:
    tender_id: str  # стабильный id, например tender-4342223
    title: str
    link: str
    source_url: str
    description: str
    tags: List[str]
    raw_html_snippet: str


def _normalize(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split()).strip()


def _tender_id_from_url(url: str) -> Optional[str]:
    """Извлекает идентификатор тендера из URL (tender-12345)."""
    path = urlparse(url).path
    m = TENDER_PATH_RE.search(path)
    if m:
        return f"tender-{m.group(1)}"
    return None


def _is_tender_page_url(url: str) -> bool:
    return _tender_id_from_url(url) is not None


def _is_market_url(url: str) -> bool:
    parsed = urlparse(url)
    if MARKET_DOMAIN not in parsed.netloc:
        return False
    return "/market/" in url


def _matches_keywords_or_tag(
    tender: Tender,
    keywords: Sequence[str],
    relevant_tag: str,
) -> bool:
    text = f"{tender.title} {' '.join(tender.tags)} {tender.description}".lower()
    for kw in keywords:
        if kw and kw.lower() in text:
            return True
    if relevant_tag and relevant_tag.lower() in text:
        return True
    for tag in tender.tags:
        if relevant_tag and relevant_tag.lower() in tag.lower():
            return True
    return False


async def fetch_page(session: aiohttp.ClientSession, url: str) -> str:
    headers = {
        "User-Agent": "TenderBot/1.0 (monitoring; +https://github.com/tenderbot)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    }
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        return await resp.text()


def _extract_market_links(html: str, base_url: str):
    """Из страницы извлекает ссылки на /market/...: отдельно тендеры и страницы категорий/списков."""
    soup = BeautifulSoup(html, "html.parser")
    base = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}"
    tender_urls = set()
    category_urls = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith("#") or "javascript:" in href:
            continue
        full_url = urljoin(base, href)
        if not _is_market_url(full_url):
            continue
        path = urlparse(full_url).path.rstrip("/")
        if "/login" in path or "/user" in path or "/search" in path:
            continue
        if _is_tender_page_url(full_url):
            tender_urls.add(full_url)
        else:
            # страница категории или списка (например /market/ или /market/category-slug/)
            if path == "" or path == "/" or path.startswith("/market"):
                category_urls.add(full_url.rstrip("/") or full_url)

    return tender_urls, category_urls


def _parse_tender_page(html: str, url: str) -> Optional[Tender]:
    """Парсит страницу одного тендера: заголовок, описание, теги."""
    tid = _tender_id_from_url(url)
    if not tid:
        return None
    soup = BeautifulSoup(html, "html.parser")

    # Заголовок: часто в h1, или в title, или в элементе с классом title/heading
    title = ""
    for el in (soup.find("h1"), soup.find("title")):
        if el and el.get_text():
            title = _normalize(el.get_text())
            break
    if not title:
        for el in soup.find_all(class_=re.compile(r"title|heading|tender-name|lot-name", re.I)):
            if el.get_text():
                title = _normalize(el.get_text())
                break
    if not title:
        title = tid

    # Описание: основной текст страницы (article, .content, .description, .lot-description)
    description_parts = []
    for sel in ("article", "[class*='content']", "[class*='description']", "[class*='lot-']", "main"):
        for el in soup.select(sel):
            if el.name == "script" or el.name == "style":
                continue
            text = _normalize(el.get_text())
            if len(text) > 50:
                description_parts.append(text[:2000])
    description = " ".join(description_parts)[:3000] if description_parts else ""

    # Теги: элементы с классом tag, category, label, badge
    tags = []
    for el in soup.find_all(class_=re.compile(r"tag|category|label|badge|theme", re.I)):
        t = _normalize(el.get_text())
        if t and len(t) < 100:
            tags.append(t)
    # Часто тег «ПО» в тексте
    for span in soup.find_all(string=re.compile(r"ПО\s*\(|программное обеспечение", re.I)):
        if hasattr(span, "strip"):
            tags.append(_normalize(span.strip())[:80])

    return Tender(
        tender_id=tid,
        title=title,
        link=url,
        source_url=url,
        description=description,
        tags=list(dict.fromkeys(tags)),
        raw_html_snippet=(title + " " + description)[:500],
    )


async def _crawl_market_pages(
    session: aiohttp.ClientSession,
    market_url: str,
    max_pages: int = 80,
) -> set[str]:
    """Обходит вложенные страницы /market/ и собирает все найденные URL страниц тендеров."""
    base = market_url.rstrip("/") or market_url
    to_visit = [base]
    visited = set()
    all_tender_urls = set()

    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        canonical = url.rstrip("/") or url
        if canonical in visited:
            continue
        visited.add(canonical)
        try:
            html = await fetch_page(session, url)
        except Exception as e:
            logger.warning("Не удалось загрузить %s: %s", url, e)
            continue
        tender_urls, category_urls = _extract_market_links(html, url)
        all_tender_urls |= tender_urls
        for u in category_urls:
            c = u.rstrip("/") or u
            if c not in visited:
                to_visit.append(u)
    return all_tender_urls


async def scrape_market(
    session: aiohttp.ClientSession,
    market_url: str,
    max_crawl_pages: int = 80,
) -> List[Tender]:
    """Обходит структуру сайта (вложенные страницы), собирает ссылки на тендеры, парсит каждую страницу тендера."""
    tender_urls = await _crawl_market_pages(session, market_url, max_pages=max_crawl_pages)
    tenders = []
    for url in tender_urls:
        try:
            html = await fetch_page(session, url)
            t = _parse_tender_page(html, url)
            if t:
                t.source_url = market_url
                tenders.append(t)
        except Exception as e:
            logger.warning("Ошибка парсинга тендера %s: %s", url, e)
    return tenders


async def scrape_extra_source(
    session: aiohttp.ClientSession,
    url: str,
) -> List[Tender]:
    """Дополнительный источник: парсим страницу, ищем ссылки на тендеры b2b-fix-price (в т.ч. .../tender-XXX/)."""
    html = await fetch_page(session, url)
    soup = BeautifulSoup(html, "html.parser")
    tenders = []
    base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    seen_links = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        full_url = urljoin(base, href)
        if full_url in seen_links:
            continue
        if MARKET_DOMAIN not in full_url or "/market/" not in full_url:
            continue
        if not _is_tender_page_url(full_url):
            continue
        seen_links.add(full_url)
        tid = _tender_id_from_url(full_url)
        if not tid:
            continue
        try:
            page_html = await fetch_page(session, full_url)
            t = _parse_tender_page(page_html, full_url)
            if t:
                t.source_url = url
                tenders.append(t)
        except Exception as e:
            logger.warning("Не удалось загрузить тендер %s из доп. источника: %s", full_url, e)
    return tenders


async def get_new_relevant_tenders(
    market_url: str,
    extra_sources: List[str],
    keywords: List[str],
    relevant_tag: str,
    max_crawl_pages: int = 80,
) -> List[Tender]:
    """Собирает тендеры по всей структуре сайта и доп. источникам, фильтрует по ключевым словам/тегу, возвращает только новые."""
    from database import is_tender_seen, mark_tender_seen

    result = []
    async with aiohttp.ClientSession() as session:
        all_tenders = []
        try:
            all_tenders.extend(
                await scrape_market(session, market_url, max_crawl_pages=max_crawl_pages)
            )
        except Exception as e:
            logger.warning("Ошибка сбора с основной площадки: %s", e)
        for src in extra_sources or []:
            try:
                all_tenders.extend(await scrape_extra_source(session, src))
            except Exception as e:
                logger.warning("Ошибка доп. источника %s: %s", src, e)

        # Дедупликация по tender_id (один и тот же тендер мог прийти с главной и из категории)
        by_id = {t.tender_id: t for t in all_tenders}

        for t in by_id.values():
            if not _matches_keywords_or_tag(t, keywords, relevant_tag):
                continue
            if await is_tender_seen(t.tender_id):
                continue
            result.append(t)
            await mark_tender_seen(t.tender_id, t.source_url)
    return result


async def get_daily_digest_data(
    market_url: str,
    extra_sources: List[str],
    keywords: List[str],
    relevant_tag: str,
    max_crawl_pages: int = 80,
):
    """
    Собирает все тендеры по структуре сайта, без фильтра «уже просмотренные».
    Возвращает (total_count, matching_tenders) для ежедневного отчёта.
    """
    async with aiohttp.ClientSession() as session:
        all_tenders = []
        try:
            all_tenders.extend(
                await scrape_market(session, market_url, max_crawl_pages=max_crawl_pages)
            )
        except Exception as e:
            logger.warning("Ошибка сбора с основной площадки: %s", e)
        for src in extra_sources or []:
            try:
                all_tenders.extend(await scrape_extra_source(session, src))
            except Exception as e:
                logger.warning("Ошибка доп. источника %s: %s", src, e)

        by_id = {t.tender_id: t for t in all_tenders}
        total = len(by_id)
        matching = [
            t for t in by_id.values()
            if _matches_keywords_or_tag(t, keywords, relevant_tag)
        ]
    return total, matching
