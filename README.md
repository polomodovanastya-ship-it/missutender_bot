# TenderBot — мониторинг тендеров в Telegram

Бот ежедневно (или с заданным интервалом) проверяет тендерную площадку [b2b-fix-price.ru/market](https://www.b2b-fix-price.ru/market/) и при обнаружении новых тендеров с ключевыми словами или тегом **«ПО (программное обеспечение)»** отправляет уведомление подписчикам в Telegram.

## Установка

1. Клонируйте или скопируйте проект, перейдите в каталог:
   ```bash
   cd TenderBot
   ```

2. Создайте виртуальное окружение и установите зависимости:
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   playwright install chromium
   ```

3. Создайте бота в Telegram через [@BotFather](https://t.me/BotFather), получите токен.

4. Создайте конфиг:
   ```bash
   cp config.example.yaml config.yaml
   ```
   Откройте `config.yaml` и укажите:
   - `telegram_bot_token` — токен от BotFather;
   - при необходимости измените `check_interval_minutes`, список `keywords` и `relevant_tag`.

## Запуск

```bash
python bot.py
```

Бот будет опрашивать площадку по расписанию и рассылать отчёт только подписанным пользователям.

### Ошибка «Cannot connect to host api.telegram.org»

Обычно это блокировка сети, отключённый VPN или «битые» переменные `HTTP_PROXY` / `HTTPS_PROXY`. Проверьте в браузере или `curl https://api.telegram.org`.

Если прямой доступ к API закрыт, включите VPN **или** пропишите в `config.yaml` параметр `telegram_proxy` (например `socks5://127.0.0.1:1080` при локальном туннеле) и установите зависимости заново (`pip install -r requirements.txt`).

### Антибот-защита площадок

Для `b2b-fix-price` и `sberbank-ast` включён fallback через headless browser (Playwright), если обычный HTTP получает антибот-страницу или пустую JS-оболочку.
Управление через `config.yaml`:
- `browser_fallback_enabled: true|false`
- `browser_headless: true|false`

## Команды бота

| Команда | Описание |
|--------|----------|
| `/start` | Приветствие и краткая справка |
| `/subscribe` | Подписаться на уведомления о новых тендерах |
| `/unsubscribe` | Отписаться от уведомлений |
| `/status` | Проверить, подписан ли пользователь |

## Конфигурация

- **telegram_bot_token** — токен Telegram-бота.
- **check_interval_minutes** — интервал проверки площадки (в минутах).
- **keywords** — список ключевых слов; если хотя бы одно встречается в названии/тегах/описании тендера, он считается релевантным.
- **relevant_tag** — тег «ПО (программное обеспечение)»; при наличии такого тега тендер тоже считается релевантным.
- **market_url** — URL страницы рынка тендеров.
- **extra_sources** — список дополнительных URL (страницы, RSS, партнёрские сайты), с которых тоже собираются ссылки на тендеры b2b-fix-price.ru.

## Данные

- Подписчики и просмотренные тендеры хранятся в SQLite-файле `tenderbot.db` в каталоге проекта.
- Файл создаётся при первом запуске.

## Важно

Структура страницы b2b-fix-price.ru может меняться. Если парсер перестанет находить тендеры, потребуется подправить логику в `scraper.py` под актуальную вёрстку страницы.
