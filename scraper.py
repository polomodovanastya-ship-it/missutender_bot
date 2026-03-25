"""Парсер тендерной площадки b2b-fix-price.ru и дополнительных источников.
Обходит вложенные страницы (категории) и собирает ссылки на страницы тендеров .../tender-<id>/.
"""
import re
import hashlib
import logging
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

# Шаблон URL страницы тендера: /market/.../tender-4342223/ или /tender-4342223
TENDER_PATH_RE = re.compile(r"/tender-(\d+)/?", re.I)

# Домен площадки для проверки ссылок
MARKET_DOMAIN = "b2b-fix-price.ru"
UTP_DOMAIN = "utp.sberbank-ast.ru"
BROWSER_FALLBACK_DOMAINS = ("b2b-fix-price.ru", "utp.sberbank-ast.ru")
BROWSER_FALLBACK_ENABLED = True
BROWSER_HEADLESS = True
DETAIL_FETCH_CONCURRENCY = 6

_PW = None
_BROWSER = None
_BROWSER_CONTEXT = None
_BROWSER_LOCK = asyncio.Lock()


@dataclass
class Tender:
    tender_id: str  # стабильный id, например tender-4342223
    title: str
    link: str
    source_url: str
    published_at: Optional[date]
    description: str
    tags: List[str]
    raw_html_snippet: str


def _normalize(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split()).strip()


def set_browser_options(enabled: bool = True, headless: bool = True) -> None:
    """Глобальные настройки browser fallback."""
    global BROWSER_FALLBACK_ENABLED, BROWSER_HEADLESS
    BROWSER_FALLBACK_ENABLED = enabled
    BROWSER_HEADLESS = headless


def set_performance_options(detail_fetch_concurrency: int = 6) -> None:
    """Глобальные настройки производительности."""
    global DETAIL_FETCH_CONCURRENCY
    DETAIL_FETCH_CONCURRENCY = max(1, int(detail_fetch_concurrency))


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




def _external_tender_id(url: str) -> str:
    return "ext-" + hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


def _looks_like_procurement_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(
        token in path
        for token in ("tender", "purchase", "zakup", "lot", "auction", "procedure", "trade")
    )


def _utp_purchase_view_id_from_url(url: str) -> Optional[str]:
    """Извлекает id из ссылки вида:
    https://utp.sberbank-ast.ru/VIP/NBT/PurchaseView/21/0/0/3892980
    """
    p = urlparse(url)
    if UTP_DOMAIN not in p.netloc.lower():
        return None
    path = p.path.strip("/")
    if "/VIP/NBT/PurchaseView/" not in path:
        return None
    last = path.split("/")[-1]
    if last.isdigit():
        return last
    return None


def _extract_publication_date(html: str) -> Optional[date]:
    """Пытается извлечь дату публикации из страницы (несколько эвристик)."""
    soup = BeautifulSoup(html, "html.parser")

    # 1) time[datetime] или time-text
    for t in soup.find_all("time"):
        raw = (t.get("datetime") or t.get_text() or "").strip()
        if not raw:
            continue
        raw_norm = raw.replace("T", " ").replace("Z", " ").strip()
        iso_match = re.search(r"(?<!\d)(\d{4})-(\d{1,2})-(\d{1,2})(?!\d)", raw_norm)
        if iso_match:
            y, m, d = map(int, iso_match.groups())
            try:
                return date(y, m, d)
            except ValueError:
                pass
        ru_match = re.search(r"(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{4})(?!\d)", raw_norm)
        if ru_match:
            d, m, y = map(int, ru_match.groups())
            try:
                return date(y, m, d)
            except ValueError:
                pass

    # 2) meta[property|name*=published*]
    for meta in soup.find_all("meta"):
        key = (meta.get("property") or meta.get("name") or "").lower()
        if "published" in key or "publication" in key or "дата" in key:
            content = (meta.get("content") or "").strip()
            iso_match = re.search(r"(?<!\d)(\d{4})-(\d{1,2})-(\d{1,2})(?!\d)", content)
            if iso_match:
                y, m, d = map(int, iso_match.groups())
                try:
                    return date(y, m, d)
                except ValueError:
                    pass
            ru_match = re.search(r"(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{4})(?!\d)", content)
            if ru_match:
                d, m, y = map(int, ru_match.groups())
                try:
                    return date(y, m, d)
                except ValueError:
                    pass

    # 3) поиск даты после ключевых слов-лейблов в тексте
    full_text = _normalize(soup.get_text(" ", strip=True))
    labels = [
        "дата публикации",
        "опубликовано",
        "дата размещения",
        "размещено",
        "дата начала",
        "дата процедуры",
        "publish",
        "publication",
    ]
    low = full_text.lower()
    for label in labels:
        pos = low.find(label)
        if pos == -1:
            continue
        snippet = full_text[pos : pos + 220]
        ru_match = re.search(r"(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{4})(?!\d)", snippet)
        if ru_match:
            d, m, y = map(int, ru_match.groups())
            try:
                return date(y, m, d)
            except ValueError:
                pass
        iso_match = re.search(r"(?<!\d)(\d{4})-(\d{1,2})-(\d{1,2})(?!\d)", snippet)
        if iso_match:
            y, m, d = map(int, iso_match.groups())
            try:
                return date(y, m, d)
            except ValueError:
                pass

    # 4) fallback: первая дата формата dd.mm.yyyy или yyyy-mm-dd
    ru_match = re.search(r"(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{4})(?!\d)", full_text)
    if ru_match:
        d, m, y = map(int, ru_match.groups())
        try:
            return date(y, m, d)
        except ValueError:
            pass
    iso_match = re.search(r"(?<!\d)(\d{4})-(\d{1,2})-(\d{1,2})(?!\d)", full_text)
    if iso_match:
        y, m, d = map(int, iso_match.groups())
        try:
            return date(y, m, d)
        except ValueError:
            pass
    return None


def _is_within_days(published_at: Optional[date], days_back: int) -> bool:
    if not published_at:
        return False
    today = datetime.now().date()
    boundary = today - timedelta(days=max(1, days_back))
    return published_at >= boundary

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


def _detect_block_page(html: str) -> Optional[str]:
    low = html.lower()
    if "servicepipe.ru" in low or "id_spinner" in low or "captcha_frame" in low:
        return "anti-bot page (ServicePipe)"
    if "действия блокированы защитой" in low or "the event id is" in low:
        return "anti-bot page (blocked by source protection)"
    return None


def _looks_like_js_shell(html: str) -> bool:
    """Эвристика: страница почти пустая и, вероятно, нужна JS-отрисовка."""
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.find_all("a", href=True)
    text_len = len(_normalize(soup.get_text(" ", strip=True)))
    return len(anchors) == 0 and text_len < 120


async def _fetch_page_headless(url: str, timeout_ms: int = 45000, headless: bool = True) -> str:
    """Загрузка страницы через headless-браузер (Playwright) с переиспользованием браузера."""
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright не установлен. Установите: pip install playwright && playwright install chromium"
        ) from exc

    global _PW, _BROWSER, _BROWSER_CONTEXT

    async with _BROWSER_LOCK:
        if _PW is None:
            _PW = await async_playwright().start()
        if _BROWSER is None:
            _BROWSER = await _PW.chromium.launch(headless=headless)
        if _BROWSER_CONTEXT is None:
            _BROWSER_CONTEXT = await _BROWSER.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            )

    page = await _BROWSER_CONTEXT.new_page()
    try:
        # domcontentloaded обычно быстрее, чем networkidle, и хватает для извлечения ссылок/текста.
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        # Для списка на b2b-fix-price часто нужен короткий скролл/подгрузка.
        if "b2b-fix-price.ru/market" in url:
            for _ in range(2):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(900)
            # Подождём, пока появятся ссылки на страницы тендеров.
            try:
                await page.wait_for_selector('a[href*="tender-"]', timeout=8000)
            except Exception:
                pass
        else:
            await page.wait_for_timeout(600)
        return await page.content()
    finally:
        await page.close()


async def fetch_page(session: aiohttp.ClientSession, url: str) -> str:
    headers = {
        "User-Agent": "TenderBot/1.0 (monitoring; +https://github.com/tenderbot)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    }
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        html = await resp.text()
    blocked = _detect_block_page(html)
    need_browser = blocked is not None or _looks_like_js_shell(html)
    domain = urlparse(url).netloc.lower()
    if (
        BROWSER_FALLBACK_ENABLED
        and need_browser
        and any(d in domain for d in BROWSER_FALLBACK_DOMAINS)
    ):
        logger.info("Headless fallback for %s", url)
        browser_html = await _fetch_page_headless(url, headless=BROWSER_HEADLESS)
        blocked_browser = _detect_block_page(browser_html)
        # Если после headless всё равно вернулась “пустая/JS-оболочка” —
        # считаем источник недоступным, чтобы отчёт не показывал фиктивные 0.
        if blocked_browser or _looks_like_js_shell(browser_html):
            raise RuntimeError(
                f"{blocked_browser or 'empty/JS-shell after headless'} for {url}"
            )
        return browser_html
    if blocked:
        raise RuntimeError(f"{blocked} for {url}")
    return html


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
        published_at=_extract_publication_date(html),
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
            # Если базовая страница не открылась, считаем источник недоступным
            if canonical == base:
                raise
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
    if not tender_urls:
        raise RuntimeError(
            f"No tender links found on market page; markup may be blocked/changed for {market_url}"
        )
    tenders: List[Tender] = []
    sem = asyncio.Semaphore(DETAIL_FETCH_CONCURRENCY)

    async def _load_one(url: str) -> Optional[Tender]:
        async with sem:
            try:
                html = await fetch_page(session, url)
                t = _parse_tender_page(html, url)
                if t:
                    t.source_url = market_url
                    return t
            except Exception as e:
                logger.warning("Ошибка парсинга тендера %s: %s", url, e)
        return None

    tasks = [_load_one(url) for url in tender_urls]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    for t in results:
        if t:
            tenders.append(t)
    return tenders


async def scrape_extra_source(
    session: aiohttp.ClientSession,
    url: str,
) -> List[Tender]:
    """Дополнительный источник: парсим страницу и ищем ссылки на закупки/тендеры."""
    html = await fetch_page(session, url)
    soup = BeautifulSoup(html, "html.parser")

    base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    seen_links = set()
    candidates: List[Tuple[str, str, str]] = []  # (full_url, tender_id, title)

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        full_url = urljoin(base, href)
        if full_url in seen_links:
            continue

        tid = None
        if MARKET_DOMAIN in full_url and "/market/" in full_url and _is_tender_page_url(full_url):
            tid = _tender_id_from_url(full_url)
        elif _utp_purchase_view_id_from_url(full_url) is not None:
            tid = "utp-" + (_utp_purchase_view_id_from_url(full_url) or "")

        if not tid:
            continue

        seen_links.add(full_url)
        title = _normalize(a.get_text()) or "Тендер"
        if len(title) < 2:
            title = "Тендер"
        candidates.append((full_url, tid, title))

    if not candidates:
        raise RuntimeError(
            f"No tender-like links found on extra source page: {url}"
        )

    sem = asyncio.Semaphore(DETAIL_FETCH_CONCURRENCY)

    async def _enrich_one(full_url: str, tid: str, title: str) -> Tender:
        description = ""
        tags: List[str] = []
        published_at: Optional[date] = None

        try:
            async with sem:
                page_html = await fetch_page(session, full_url)
        except Exception as e:
            logger.warning("Не удалось загрузить тендер %s из доп. источника: %s", full_url, e)
            return Tender(
                tender_id=tid,
                title=title,
                link=full_url,
                source_url=url,
                published_at=None,
                description="",
                tags=[],
                raw_html_snippet=(title + " ")[:500],
            )

        # b2b-fix-price tender страницы — парсим как раньше
        if MARKET_DOMAIN in full_url and _is_tender_page_url(full_url):
            parsed = _parse_tender_page(page_html, full_url)
            if parsed:
                parsed.tender_id = tid
                parsed.source_url = url
                return parsed

        # UTP: попробуем улучшить title (h1/title)
        if UTP_DOMAIN in full_url:
            soup2 = BeautifulSoup(page_html, "html.parser")
            h1 = soup2.find("h1")
            if h1 and h1.get_text():
                title = _normalize(h1.get_text())
            if (not title or title == "Тендер") and soup2.title and soup2.title.get_text():
                title = _normalize(soup2.title.get_text())

        # для внешних площадок: хотя бы дата публикации + короткое описание для keyword-матчинга
        published_at = _extract_publication_date(page_html)
        soup2 = BeautifulSoup(page_html, "html.parser")
        meta_desc = soup2.find("meta", attrs={"name": re.compile(r"description", re.I)})
        if meta_desc and meta_desc.get("content"):
            description = _normalize(meta_desc.get("content"))[:1200]
        else:
            main = soup2.find("main") or soup2.find("article") or soup2.body
            if main:
                text = _normalize(main.get_text(" ", strip=True))
                description = text[:1200]

        return Tender(
            tender_id=tid,
            title=title,
            link=full_url,
            source_url=url,
            published_at=published_at,
            description=description,
            tags=tags,
            raw_html_snippet=(title + " " + description)[:500],
        )

    tasks = [_enrich_one(full_url, tid, title) for full_url, tid, title in candidates]
    tenders = await asyncio.gather(*tasks, return_exceptions=False)
    return tenders


async def get_new_relevant_tenders(
    market_url: str,
    extra_sources: List[str],
    keywords: List[str],
    relevant_tag: str,
    max_crawl_pages: int = 80,
    days_back: int = 7,
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
        by_id = {
            t.tender_id: t for t in all_tenders
            if _is_within_days(t.published_at, days_back)
        }

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
    days_back: int = 7,
) -> Tuple[Optional[int], List[Tender], Dict[str, str], List[Tender]]:
    """
    Собирает все тендеры по структуре сайта, без фильтра «уже просмотренные».
    Возвращает (total_count_or_None, matching_tenders, source_status, freshest_tenders).
    Если все источники недоступны, total_count_or_None == None.
    """
    source_status: Dict[str, str] = {}

    async with aiohttp.ClientSession() as session:
        all_tenders: List[Tender] = []
        successful_sources = 0

        try:
            market_tenders = await scrape_market(session, market_url, max_crawl_pages=max_crawl_pages)
            all_tenders.extend(market_tenders)
            source_status[market_url] = f"ok ({len(market_tenders)})"
            successful_sources += 1
        except Exception as e:
            source_status[market_url] = f"error ({e})"
            logger.warning("Ошибка сбора с основной площадки: %s", e)

        for src in extra_sources or []:
            try:
                src_tenders = await scrape_extra_source(session, src)
                all_tenders.extend(src_tenders)
                source_status[src] = f"ok ({len(src_tenders)})"
                successful_sources += 1
            except Exception as e:
                source_status[src] = f"error ({e})"
                logger.warning("Ошибка доп. источника %s: %s", src, e)

        if successful_sources == 0:
            return None, [], source_status, []

        by_id = {
            t.tender_id: t for t in all_tenders
            if _is_within_days(t.published_at, days_back)
        }
        total = len(by_id)
        matching = [
            t for t in by_id.values()
            if _matches_keywords_or_tag(t, keywords, relevant_tag)
        ]
        freshest_candidates = [t for t in by_id.values() if t.published_at is not None]
        freshest = sorted(
            freshest_candidates,
            key=lambda t: t.published_at,
            reverse=True,
        )[:5]
    return total, matching, source_status, freshest
