"""
Kufar.by Resale Bot
===================
Телеграм-бот для поиска товаров на Kufar.by (Орша, Барань) с AI-анализом маржи.

Требования:
    pip install aiogram==3.15.0 aiosqlite aiohttp apscheduler python-dotenv beautifulsoup4 lxml

Переменные окружения (.env):
    BOT_TOKEN=your_telegram_bot_token
    OWNER_ID=your_telegram_id
    YANDEX_API_KEY=your_yandex_gpt_api_key        (опционально)
    YANDEX_FOLDER_ID=your_yandex_cloud_folder_id  (опционально)
    DEEPSEEK_API_KEY=your_deepseek_api_key        (опционально, fallback)
"""

import os
import json
import logging
import asyncio
import re
import aiosqlite
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import aiohttp
from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY")
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

DB_PATH = "kufar_bot.db"

CITIES = {
    "орша": "orsha",
    "барань": "baran",
}

CATEGORIES_SLUG = {
    "компьютерная_техника": "kompyuternaya-tehnika",
    "телефоны_и_планшеты": "telefony-i-planshety",
    "часы": "chasy",
    "наручные_часы": "chasy",
}

HEADERS_KUFAR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("kufar_bot")


# =============================================================================
# МОДЕЛИ ДАННЫХ
# =============================================================================

@dataclass
class Product:
    ad_id: str
    title: str
    price: float
    currency: str
    description: str
    images: List[str]
    location: str
    url: str
    category: str
    region: str
    seller_name: str
    published_at: str
    avg_market_price: Optional[float] = None
    margin_percent: Optional[float] = None
    recommendation: Optional[str] = None
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    price_history: Optional[str] = None
    notified: bool = False


# =============================================================================
# БАЗА ДАННЫХ
# =============================================================================

class Database:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    ad_id TEXT PRIMARY KEY,
                    title TEXT,
                    price REAL,
                    currency TEXT,
                    description TEXT,
                    images TEXT,
                    location TEXT,
                    url TEXT,
                    category TEXT,
                    region TEXT,
                    seller_name TEXT,
                    published_at TEXT,
                    avg_market_price REAL,
                    margin_percent REAL,
                    recommendation TEXT,
                    first_seen TEXT,
                    last_seen TEXT,
                    price_history TEXT,
                    notified INTEGER DEFAULT 0
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS price_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ad_id TEXT,
                    old_price REAL,
                    new_price REAL,
                    alert_time TEXT,
                    FOREIGN KEY (ad_id) REFERENCES products(ad_id)
                )
            """)
            await db.commit()

    async def save_product(self, product: Product) -> Dict[str, Any]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT price, price_history, first_seen FROM products WHERE ad_id = ?",
                (product.ad_id,)
            )
            row = await cursor.fetchone()

            now = datetime.now().isoformat()

            if row is None:
                product.first_seen = now
                product.last_seen = now
                product.price_history = json.dumps([{"price": product.price, "date": now}])

                await db.execute("""
                    INSERT INTO products 
                    (ad_id, title, price, currency, description, images, location, url,
                     category, region, seller_name, published_at, avg_market_price,
                     margin_percent, recommendation, first_seen, last_seen, price_history, notified)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    product.ad_id, product.title, product.price, product.currency,
                    product.description, json.dumps(product.images), product.location,
                    product.url, product.category, product.region, product.seller_name,
                    product.published_at, product.avg_market_price, product.margin_percent,
                    product.recommendation, product.first_seen, product.last_seen,
                    product.price_history, 0
                ))
                await db.commit()
                return {"status": "new", "product": product}

            else:
                old_price, price_history_json, first_seen = row
                price_history = json.loads(price_history_json or "[]")

                if product.price < old_price:
                    price_history.append({"price": product.price, "date": now})
                    await db.execute("""
                        UPDATE products SET 
                            price = ?, 
                            last_seen = ?,
                            price_history = ?,
                            notified = 0
                        WHERE ad_id = ?
                    """, (product.price, now, json.dumps(price_history), product.ad_id))

                    await db.execute("""
                        INSERT INTO price_alerts (ad_id, old_price, new_price, alert_time)
                        VALUES (?, ?, ?, ?)
                    """, (product.ad_id, old_price, product.price, now))

                    await db.commit()

                    product.first_seen = first_seen
                    product.last_seen = now
                    product.price_history = json.dumps(price_history)
                    return {
                        "status": "price_dropped",
                        "product": product,
                        "old_price": old_price,
                        "new_price": product.price
                    }

                else:
                    price_history.append({"price": product.price, "date": now})
                    await db.execute("""
                        UPDATE products SET 
                            last_seen = ?,
                            price_history = ?
                        WHERE ad_id = ?
                    """, (now, json.dumps(price_history[-20:]), product.ad_id))
                    await db.commit()
                    return {"status": "updated", "product": product}

    async def get_products_by_budget(self, max_price: float, category: Optional[str] = None) -> List[Product]:
        async with aiosqlite.connect(self.db_path) as db:
            if category:
                cursor = await db.execute(
                    """SELECT * FROM products 
                       WHERE price <= ? AND category = ? AND price > 0 
                       ORDER BY margin_percent DESC NULLS LAST""",
                    (max_price, category)
                )
            else:
                cursor = await db.execute(
                    """SELECT * FROM products 
                       WHERE price <= ? AND price > 0 
                       ORDER BY margin_percent DESC NULLS LAST""",
                    (max_price,)
                )
            rows = await cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            products = []
            for row in rows:
                data = dict(zip(columns, row))
                data["images"] = json.loads(data.get("images", "[]"))
                data["notified"] = bool(data.get("notified", 0))
                products.append(Product(**{k: v for k, v in data.items() if k in Product.__dataclass_fields__}))
            return products

    async def get_unnotified_drops(self) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT p.*, pa.old_price, pa.new_price 
                FROM products p
                JOIN price_alerts pa ON p.ad_id = pa.ad_id
                WHERE p.notified = 0
                ORDER BY pa.alert_time DESC
            """)
            rows = await cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    async def mark_notified(self, ad_id: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE products SET notified = 1 WHERE ad_id = ?", (ad_id,))
            await db.commit()

    async def get_stats(self) -> Dict[str, Any]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM products WHERE price > 0")
            total = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT AVG(price) FROM products WHERE price > 0")
            avg_price = (await cursor.fetchone())[0] or 0

            cursor = await db.execute("""
                SELECT category, COUNT(*) as cnt, AVG(price) as avg_price 
                FROM products WHERE price > 0 GROUP BY category
            """)
            by_category = await cursor.fetchall()

            cursor = await db.execute("""
                SELECT COUNT(*) FROM price_alerts 
                WHERE alert_time > ?
            """, ((datetime.now() - timedelta(days=1)).isoformat(),))
            drops_24h = (await cursor.fetchone())[0]

            return {
                "total_products": total,
                "avg_price": round(avg_price, 2),
                "by_category": by_category,
                "price_drops_24h": drops_24h
            }

    async def get_setting(self, key: str, default: Any = None) -> Any:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else default

    async def set_setting(self, key: str, value: Any):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, json.dumps(value))
            )
            await db.commit()


# =============================================================================
# ПАРСЕР KUFAR.BY — HTML КАРТОЧКИ + СТРАНИЦА ТОВАРА ДЛЯ ОПИСАНИЯ
# =============================================================================

class KufarParser:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=HEADERS_KUFAR)
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    def _build_listing_url(self, city_slug: str, category_slug: str) -> str:
        return f"https://www.kufar.by/l/r~{city_slug}/{category_slug}?sort=lst.d"

    def _extract_ad_id_from_href(self, href: str) -> Optional[str]:
        match = re.search(r'/item/(\d+)', href)
        return match.group(1) if match else None

    def _parse_price(self, price_text: str) -> tuple[float, str]:
        """Парсит цену из текста вида '10 р.', '500 BYN', 'Договорная'."""
        if not price_text:
            return 0.0, "BYN"
        price_lower = price_text.lower()
        if "договорная" in price_lower or "бесплатно" in price_lower:
            return 0.0, "BYN"
        # Ищем числа
        numbers = re.findall(r'[\d\s]+', price_text.replace(" ", ""))
        if numbers:
            try:
                val = float(numbers[0])
                if "р." in price_lower or "руб" in price_lower:
                    return val, "BYN"
                if "$" in price_text or "usd" in price_lower:
                    return val * 3.25, "BYN"
                return val, "BYN"
            except ValueError:
                pass
        return 0.0, "BYN"

    def _extract_ads_from_listing_html(self, html: str) -> List[Dict]:
        """Парсит карточки объявлений с листинговой страницы."""
        ads = []
        soup = BeautifulSoup(html, "lxml")

        # Ищем все карточки-ссылки
        cards = soup.find_all("a", attrs={"data-testid": "kufar-ad"})
        logger.info(f"Найдено {len(cards)} карточек на странице")

        for card in cards:
            try:
                href = card.get("href", "")
                ad_id = self._extract_ad_id_from_href(href)
                if not ad_id:
                    continue

                # Заголовок
                title_tag = card.find("h3", class_=re.compile(r"styles_title__"))
                title = title_tag.get_text(strip=True) if title_tag else "Без названия"

                # Цена
                price_tag = card.find("p", class_=re.compile(r"styles_price__"))
                price_text = price_tag.get_text(strip=True) if price_tag else ""
                price, currency = self._parse_price(price_text)

                # Локация
                location_tag = card.find("p", class_=re.compile(r"styles_region__"))
                location = location_tag.get_text(strip=True) if location_tag else "Не указано"

                # Изображение
                images = []
                img_tag = card.find("img")
                if img_tag:
                    img_src = img_tag.get("src") or img_tag.get("data-src")
                    if img_src:
                        images.append(img_src)

                # Дата (если есть)
                date_tag = card.find("span")
                published = datetime.now().isoformat()

                ads.append({
                    "ad_id": ad_id,
                    "title": title,
                    "price": price,
                    "currency": currency,
                    "location": location,
                    "images": images,
                    "url": href.split("?")[0] if "?" in href else href,
                    "published_at": published,
                })
            except Exception as e:
                logger.debug(f"Ошибка парсинга карточки: {e}")
                continue

        return ads

    async def _fetch_ad_detail(self, ad_id: str) -> Dict[str, Any]:
        """
        Заходит на страницу товара и забирает описание + имя продавца.
        Возвращает dict с description и seller_name.
        """
        url = f"https://www.kufar.by/item/{ad_id}"
        result = {"description": "", "seller_name": "Неизвестно", "images": []}

        try:
            async with self.session.get(url, timeout=20) as resp:
                if resp.status != 200:
                    logger.warning(f"Страница товара {ad_id} вернула {resp.status}")
                    return result

                html = await resp.text()
                soup = BeautifulSoup(html, "lxml")

                # Способ 1: __NEXT_DATA__ (Next.js)
                next_data = soup.find("script", id="__NEXT_DATA__", type="application/json")
                if next_data and next_data.string:
                    try:
                        data = json.loads(next_data.string)
                        page_props = data.get("props", {}).get("pageProps", {})
                        initial_state = page_props.get("initialState", {})

                        # Ищем объявление в разных местах
                        ad_data = None
                        if "ad" in initial_state:
                            ad_data = initial_state["ad"]
                        elif "ads" in initial_state and isinstance(initial_state["ads"], dict):
                            ad_data = initial_state["ads"].get(str(ad_id))
                        elif "item" in initial_state:
                            ad_data = initial_state["item"]

                        if ad_data and isinstance(ad_data, dict):
                            result["description"] = ad_data.get("body") or ad_data.get("description") or ""
                            # Продавец
                            account = ad_data.get("account_parameters", [])
                            for ap in account:
                                if ap.get("p") == "name":
                                    result["seller_name"] = ap.get("v", "Неизвестно")
                                    break
                            # Доп. фото
                            imgs = ad_data.get("images", [])
                            for img in imgs:
                                if isinstance(img, dict):
                                    src = img.get("path") or img.get("url") or img.get("large") or img.get("src")
                                    if src:
                                        if not src.startswith("http"):
                                            src = f"https:{src}"
                                        result["images"].append(src)
                                elif isinstance(img, str):
                                    result["images"].append(img if img.startswith("http") else f"https:{img}")
                            return result
                    except Exception as e:
                        logger.debug(f"__NEXT_DATA__ detail parse error: {e}")

                # Способ 2: HTML-разметка страницы товара
                # Описание
                desc_div = soup.find("div", attrs={"data-name": "ad_description"})
                if desc_div:
                    result["description"] = desc_div.get_text(strip=True)
                else:
                    # Пробуем найти по классам
                    for cls in ["styles_description__", "styles_body__", "description"]:
                        tag = soup.find("div", class_=re.compile(cls))
                        if tag:
                            result["description"] = tag.get_text(strip=True)
                            break

                # Продавец
                seller_tag = soup.find("div", attrs={"data-name": "seller_name"})
                if seller_tag:
                    result["seller_name"] = seller_tag.get_text(strip=True)

                # Фото
                for img in soup.find_all("img", class_=re.compile(r"styles_image__")):
                    src = img.get("src") or img.get("data-src")
                    if src and "list_thumbs" not in src:
                        result["images"].append(src)

        except Exception as e:
            logger.error(f"Ошибка загрузки страницы товара {ad_id}: {e}")

        return result

    async def search_ads(
        self,
        city_slug: str = "orsha",
        category_slug: str = "kompyuternaya-tehnika",
        limit: int = 20,
    ) -> List[Product]:
        listing_url = self._build_listing_url(city_slug, category_slug)
        logger.info(f"Запрос листинга: {listing_url}")

        try:
            async with self.session.get(listing_url, timeout=30) as resp:
                if resp.status != 200:
                    logger.error(f"HTTP {resp.status} для {listing_url}")
                    return []

                html = await resp.text()
                raw_ads = self._extract_ads_from_listing_html(html)

                products = []
                for raw in raw_ads[:limit]:
                    # Пропускаем договорные цены
                    if raw["price"] <= 0:
                        continue

                    # Загружаем детали со страницы товара (описание + продавец + фото)
                    detail = await self._fetch_ad_detail(raw["ad_id"])

                    # Объединяем фото (превью + детальные)
                    all_images = raw["images"][:1] + detail["images"][:4]
                    # Убираем дубликаты
                    seen = set()
                    unique_images = []
                    for img in all_images:
                        if img not in seen:
                            seen.add(img)
                            unique_images.append(img)

                    product = Product(
                        ad_id=raw["ad_id"],
                        title=raw["title"],
                        price=raw["price"],
                        currency=raw["currency"],
                        description=detail["description"][:3000],
                        images=unique_images[:5],
                        location=raw["location"],
                        url=raw["url"],
                        category=category_slug,
                        region=city_slug,
                        seller_name=detail["seller_name"],
                        published_at=raw["published_at"],
                    )
                    products.append(product)

                logger.info(f"Успешно распарсено {len(products)} товаров с {listing_url}")
                return products

        except Exception as e:
            logger.error(f"Ошибка запроса {listing_url}: {e}")
            return []


# =============================================================================
# AI АНАЛИЗАТОР (YandexGPT + DeepSeek fallback)
# =============================================================================

class AIAnalyzer:
    def __init__(self):
        self.provider = None
        self.yandex_key = YANDEX_API_KEY
        self.folder_id = YANDEX_FOLDER_ID
        self.deepseek_key = DEEPSEEK_API_KEY

        if self.yandex_key and self.folder_id:
            self.provider = "yandex"
            logger.info("AI: используется YandexGPT")
        elif self.deepseek_key:
            self.provider = "deepseek"
            logger.info("AI: используется DeepSeek (fallback)")
        else:
            logger.warning("AI: провайдер не настроен")

    async def analyze_product(self, product: Product, session: aiohttp.ClientSession) -> Dict[str, Any]:
        if self.provider == "yandex":
            return await self._analyze_yandex(product, session)
        elif self.provider == "deepseek":
            return await self._analyze_deepseek(product, session)
        else:
            return {
                "avg_market_price": None,
                "margin_percent": None,
                "recommendation": "AI-анализ отключен (нет API-ключа)"
            }

    def _build_prompt(self, product: Product) -> str:
        return f"""
Ты — эксперт по перепродаже товаров на белорусских барахолках (Kufar.by, Onliner, etc.).
Проанализируй объявление и оцени маржу перепродажи.

Товар: {product.title}
Цена: {product.price} {product.currency}
Описание: {product.description[:1500]}
Город: {product.location}

Задача:
1. Оцени среднерыночную цену этого товара на белорусских площадках (Kufar, Onliner, барохолки).
2. Рассчитай примерную маржу в процентах.
3. Дай рекомендацию: стоит ли покупать для перепродажи.

Ответь СТРОГО в формате JSON:
{{
    "avg_market_price": число (средняя рыночная цена в BYN),
    "margin_percent": число (маржа в процентах, может быть отрицательной),
    "recommendation": "краткая рекомендация на русском языке (1-2 предложения)"
}}
"""

    def _parse_ai_json(self, text: str) -> Dict[str, Any]:
        try:
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                result = json.loads(text[start:end])
                return {
                    "avg_market_price": float(result.get("avg_market_price", 0)) or None,
                    "margin_percent": float(result.get("margin_percent", 0)) or None,
                    "recommendation": str(result.get("recommendation", "Нет рекомендации"))
                }
        except Exception:
            pass
        return {
            "avg_market_price": None,
            "margin_percent": None,
            "recommendation": text[:500]
        }

    async def _analyze_yandex(self, product: Product, session: aiohttp.ClientSession) -> Dict[str, Any]:
        prompt = self._build_prompt(product)
        payload = {
            "modelUri": f"gpt://{self.folder_id}/yandexgpt/latest",
            "completionOptions": {
                "stream": False,
                "temperature": 0.3,
                "maxTokens": 500
            },
            "messages": [
                {"role": "system", "text": "Ты — эксперт по перепродаже товаров в Беларуси."},
                {"role": "user", "text": prompt}
            ]
        }
        headers = {
            "Authorization": f"Api-Key {self.yandex_key}",
            "Content-Type": "application/json",
        }
        try:
            async with session.post(
                "https://llm.api.cloud.yandex.net/foundationModels/v1/completion",
                json=payload,
                headers=headers,
                timeout=60
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"YandexGPT error: {resp.status} - {text}")
                    return {"avg_market_price": None, "margin_percent": None, "recommendation": f"Ошибка Yandex: {resp.status}"}
                data = await resp.json()
                result_text = data.get("result", {}).get("alternatives", [{}])[0].get("message", {}).get("text", "")
                return self._parse_ai_json(result_text)
        except Exception as e:
            logger.error(f"Yandex analyze error: {e}")
            return {"avg_market_price": None, "margin_percent": None, "recommendation": f"Ошибка: {e}"}

    async def _analyze_deepseek(self, product: Product, session: aiohttp.ClientSession) -> Dict[str, Any]:
        prompt = self._build_prompt(product)
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": "Ты — эксперт по перепродаже товаров в Беларуси."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.3,
            "max_tokens": 500
        }
        headers = {
            "Authorization": f"Bearer {self.deepseek_key}",
            "Content-Type": "application/json",
        }
        try:
            async with session.post(
                "https://api.deepseek.com/chat/completions",
                json=payload,
                headers=headers,
                timeout=60
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"DeepSeek error: {resp.status} - {text}")
                    return {"avg_market_price": None, "margin_percent": None, "recommendation": f"Ошибка DeepSeek: {resp.status}"}
                data = await resp.json()
                result_text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                return self._parse_ai_json(result_text)
        except Exception as e:
            logger.error(f"DeepSeek analyze error: {e}")
            return {"avg_market_price": None, "margin_percent": None, "recommendation": f"Ошибка: {e}"}


# =============================================================================
# СЕРВИС БОТА
# =============================================================================

class BotService:
    def __init__(self, bot: Bot, db: Database, analyzer: AIAnalyzer):
        self.bot = bot
        self.db = db
        self.analyzer = analyzer
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        self.session = aiohttp.ClientSession()

    async def stop(self):
        if self.session:
            await self.session.close()

    async def run_parsing_cycle(self):
        logger.info("=" * 50)
        logger.info("Запуск цикла парсинга Kufar.by")

        settings = await self.db.get_setting("parser_config", {
            "locations": [
                {"city": "orsha", "category": "kompyuternaya-tehnika"},
                {"city": "orsha", "category": "telefony-i-planshety"},
                {"city": "orsha", "category": "chasy"},
                {"city": "baran", "category": "kompyuternaya-tehnika"},
                {"city": "baran", "category": "telefony-i-planshety"},
                {"city": "baran", "category": "chasy"},
            ],
            "max_price": 5000,
            "min_price": 50,
        })

        new_count = 0
        drop_count = 0

        async with KufarParser() as parser:
            for loc in settings.get("locations", []):
                city = loc.get("city", "orsha")
                category = loc.get("category", "kompyuternaya-tehnika")

                logger.info(f"Парсим: город={city}, категория={category}")

                ads = await parser.search_ads(
                    city_slug=city,
                    category_slug=category,
                    limit=15,  # Меньше, т.к. каждый товар = 2 запроса (листинг + деталь)
                )

                for ad in ads:
                    # Фильтр по цене
                    if settings.get("max_price") and ad.price > settings["max_price"]:
                        continue
                    if settings.get("min_price") and ad.price < settings["min_price"]:
                        continue

                    try:
                        ai_result = await self.analyzer.analyze_product(ad, self.session)
                        ad.avg_market_price = ai_result.get("avg_market_price")
                        ad.margin_percent = ai_result.get("margin_percent")
                        ad.recommendation = ai_result.get("recommendation")
                    except Exception as e:
                        logger.error(f"AI error for {ad.ad_id}: {e}")

                    result = await self.db.save_product(ad)

                    if result["status"] == "new":
                        new_count += 1
                        await self._notify_new_product(result["product"])
                    elif result["status"] == "price_dropped":
                        drop_count += 1
                        await self._notify_price_drop(
                            result["product"],
                            result["old_price"],
                            result["new_price"]
                        )

        logger.info(f"Цикл завершён. Новых: {new_count}, Снижений цены: {drop_count}")
        return {"new": new_count, "drops": drop_count}

    async def _notify_new_product(self, product: Product):
        try:
            text = self._format_product_message(product, is_new=True)
            keyboard = self._product_keyboard(product)

            if product.images:
                try:
                    await self.bot.send_photo(
                        chat_id=OWNER_ID,
                        photo=product.images[0],
                        caption=text,
                        reply_markup=keyboard
                    )
                except Exception as img_err:
                    logger.warning(f"Не удалось отправить фото: {img_err}")
                    await self.bot.send_message(chat_id=OWNER_ID, text=text, reply_markup=keyboard)
            else:
                await self.bot.send_message(chat_id=OWNER_ID, text=text, reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Notify error: {e}")

    async def _notify_price_drop(self, product: Product, old_price: float, new_price: float):
        try:
            drop_percent = round((old_price - new_price) / old_price * 100, 1) if old_price else 0
            text = (
                f"🔥 <b>ЦЕНА СНИЗИЛАСЬ!</b>\n\n"
                f"📱 {product.title}\n"
                f"💰 Старая цена: <s>{old_price} {product.currency}</s>\n"
                f"💰 Новая цена: <b>{new_price} {product.currency}</b>\n"
                f"📉 Снижение: <b>-{drop_percent}%</b>\n\n"
                f"📍 {product.location}\n"
                f"🔗 <a href='{product.url}'>Открыть на Kufar</a>\n\n"
                f"🤖 Рекомендация AI: {product.recommendation or '—'}"
            )

            if product.images:
                try:
                    await self.bot.send_photo(chat_id=OWNER_ID, photo=product.images[0], caption=text)
                except Exception as img_err:
                    logger.warning(f"Фото не отправлено: {img_err}")
                    await self.bot.send_message(chat_id=OWNER_ID, text=text)
            else:
                await self.bot.send_message(chat_id=OWNER_ID, text=text)

            await self.db.mark_notified(product.ad_id)
        except Exception as e:
            logger.error(f"Price drop notify error: {e}")

    def _format_product_message(self, product: Product, is_new: bool = False) -> str:
        emoji = "🆕" if is_new else "📦"
        margin_text = ""
        if product.margin_percent is not None:
            margin_emoji = "🟢" if product.margin_percent > 20 else "🟡" if product.margin_percent > 5 else "🔴"
            margin_text = f"\n{margin_emoji} Маржа: <b>{product.margin_percent:+.1f}%</b>"

        avg_text = ""
        if product.avg_market_price:
            avg_text = f"\n📊 Средняя цена на рынке: <b>{product.avg_market_price:.0f} {product.currency}</b>"

        rec_text = ""
        if product.recommendation:
            rec_text = f"\n\n🤖 <i>{product.recommendation}</i>"

        # Описание (обрезаем для Telegram)
        desc_text = ""
        if product.description:
            desc = product.description[:500]
            if len(product.description) > 500:
                desc += "..."
            desc_text = f"\n\n📝 <b>Описание:</b>\n<i>{desc}</i>"

        return (
            f"{emoji} <b>{product.title}</b>\n\n"
            f"💰 Цена: <b>{product.price:.0f} {product.currency}</b>"
            f"{avg_text}"
            f"{margin_text}\n"
            f"📍 {product.location}\n"
            f"👤 Продавец: {product.seller_name}\n"
            f"🔗 <a href='{product.url}'>Открыть на Kufar</a>"
            f"{desc_text}"
            f"{rec_text}"
        )

    def _product_keyboard(self, product: Product) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔗 Открыть", url=product.url),
                InlineKeyboardButton(
                    text="💰 По бюджету",
                    callback_data=f"budget_{product.price:.0f}"
                )
            ]
        ])

    async def send_products_by_budget(self, max_price: float, category: Optional[str] = None):
        products = await self.db.get_products_by_budget(max_price, category)

        if not products:
            await self.bot.send_message(
                OWNER_ID,
                f"😕 Нет товаров до <b>{max_price:.0f} BYN</b>"
            )
            return

        await self.bot.send_message(
            OWNER_ID,
            f"📋 Найдено <b>{len(products)}</b> товаров до <b>{max_price:.0f} BYN</b>:"
        )

        for product in products[:20]:
            try:
                text = self._format_product_message(product)
                if product.images:
                    try:
                        await self.bot.send_photo(OWNER_ID, photo=product.images[0], caption=text)
                    except Exception as img_err:
                        logger.warning(f"Фото не отправлено: {img_err}")
                        await self.bot.send_message(OWNER_ID, text=text)
                else:
                    await self.bot.send_message(OWNER_ID, text=text)
            except Exception as e:
                logger.error(f"Send budget product error: {e}")

    async def send_stats(self):
        stats = await self.db.get_stats()
        text = (
            f"📊 <b>Статистика бота</b>\n\n"
            f"📦 Всего товаров в базе: <b>{stats['total_products']}</b>\n"
            f"💰 Средняя цена: <b>{stats['avg_price']:.0f} BYN</b>\n"
            f"🔥 Снижений цены за 24ч: <b>{stats['price_drops_24h']}</b>\n\n"
            f"<b>По категориям:</b>\n"
        )
        for cat, cnt, avg in stats["by_category"]:
            cat_name = {v: k for k, v in CATEGORIES_SLUG.items()}.get(cat, cat)
            text += f"• {cat_name}: {cnt} шт. (средняя {avg:.0f} BYN)\n"

        await self.bot.send_message(OWNER_ID, text)

    async def send_price_drops(self):
        drops = await self.db.get_unnotified_drops()
        if not drops:
            await self.bot.send_message(OWNER_ID, "✅ Нет новых снижений цены.")
            return

        for drop in drops[:10]:
            try:
                old = drop.get("old_price", 0)
                new = drop.get("new_price", 0)
                drop_pct = round((old - new) / old * 100, 1) if old else 0
                text = (
                    f"🔥 <b>СНИЖЕНИЕ ЦЕНЫ</b>\n\n"
                    f"📱 {drop.get('title', 'Без названия')}\n"
                    f"💰 <s>{old:.0f}</s> → <b>{new:.0f} {drop.get('currency', 'BYN')}</b> "
                    f"(-{drop_pct}%)\n"
                    f"🔗 <a href='{drop.get('url', '')}'>Открыть</a>"
                )
                images = json.loads(drop.get("images", "[]"))
                if images:
                    try:
                        await self.bot.send_photo(OWNER_ID, photo=images[0], caption=text)
                    except Exception as img_err:
                        logger.warning(f"Фото не отправлено: {img_err}")
                        await self.bot.send_message(OWNER_ID, text)
                else:
                    await self.bot.send_message(OWNER_ID, text)
                await self.db.mark_notified(drop["ad_id"])
            except Exception as e:
                logger.error(f"Send drop error: {e}")


# =============================================================================
# MIDDLEWARE ДЛЯ DEPENDENCY INJECTION
# =============================================================================

class DIMiddleware(BaseMiddleware):
    def __init__(self, service: BotService, db: Database):
        self.service = service
        self.db = db
        super().__init__()

    async def __call__(self, handler, event, data):
        data["service"] = self.service
        data["db"] = self.db
        return await handler(event, data)


# =============================================================================
# УТИЛИТА: Безопасное получение конфига с инициализацией
# =============================================================================

def ensure_config(config: Optional[Dict]) -> Dict:
    default = {
        "locations": [
            {"city": "orsha", "category": "kompyuternaya-tehnika"},
            {"city": "orsha", "category": "telefony-i-planshety"},
            {"city": "orsha", "category": "chasy"},
            {"city": "baran", "category": "kompyuternaya-tehnika"},
            {"city": "baran", "category": "telefony-i-planshety"},
            {"city": "baran", "category": "chasy"},
        ],
        "max_price": 5000,
        "min_price": 50,
    }
    if not config:
        return default
    if "locations" not in config:
        config["locations"] = default["locations"]
    return config


# =============================================================================
# TELEGRAM РОУТЕР
# =============================================================================

router = Router()

OWNER_FILTER = F.from_user.id == OWNER_ID

@router.message(Command("start"), OWNER_FILTER)
async def cmd_start(message: Message):
    await message.answer(
        "👋 <b>Kufar Resale Bot</b> запущен!\n\n"
        "Команды:\n"
        "/parse — запустить парсинг сейчас\n"
        "/budget <сумма> — товары до указанной суммы\n"
        "/stats — статистика\n"
        "/drops — товары с снижением цены\n"
        "/config — настройки парсера\n"
        "/help — справка"
    )

@router.message(Command("help"), OWNER_FILTER)
async def cmd_help(message: Message):
    await message.answer(
        "<b>📖 Справка</b>\n\n"
        "<b>/parse</b> — ручной запуск парсинга\n"
        "<b>/budget 1000</b> — товары до 1000 BYN\n"
        "<b>/stats</b> — статистика по базе\n"
        "<b>/drops</b> — снижения цен\n"
        "<b>/config</b> — текущие настройки\n"
        "<b>/setcity орша</b> — добавить город\n"
        "<b>/setcategory компьютерная_техника</b> — добавить категорию\n"
        "<b>/setmaxprice 5000</b> — макс. цена\n"
        "<b>/setminprice 50</b> — мин. цена\n\n"
        "⏱️ Автопарсинг: каждые 20 минут\n"
        "🤖 AI: YandexGPT / DeepSeek\n"
        "🌍 Города: Орша, Барань\n"
        "📂 Категории: компьютерная_техника, телефоны_и_планшеты, часы"
    )

@router.message(Command("parse"), OWNER_FILTER)
async def cmd_parse(message: Message, service: BotService):
    msg = await message.answer("⏳ Запускаю парсинг...")
    try:
        result = await service.run_parsing_cycle()
        await msg.edit_text(
            f"✅ Парсинг завершён!\n"
            f"🆕 Новых товаров: <b>{result['new']}</b>\n"
            f"🔥 Снижений цены: <b>{result['drops']}</b>"
        )
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка: {e}")

@router.message(Command("budget"), OWNER_FILTER)
async def cmd_budget(message: Message, service: BotService):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("❌ Укажите сумму: /budget 1000")
        return

    try:
        max_price = float(parts[1])
    except ValueError:
        await message.answer("❌ Неверная сумма")
        return

    category = None
    if len(parts) > 2:
        cat_input = parts[2].lower()
        category = CATEGORIES_SLUG.get(cat_input)

    await message.answer(f"🔍 Ищу товары до {max_price:.0f} BYN...")
    await service.send_products_by_budget(max_price, category)

@router.message(Command("stats"), OWNER_FILTER)
async def cmd_stats(message: Message, service: BotService):
    await service.send_stats()

@router.message(Command("drops"), OWNER_FILTER)
async def cmd_drops(message: Message, service: BotService):
    await service.send_price_drops()

@router.message(Command("config"), OWNER_FILTER)
async def cmd_config(message: Message, db: Database):
    raw_config = await db.get_setting("parser_config")
    config = ensure_config(raw_config)

    locs = config.get("locations", [])
    text_lines = ["⚙️ <b>Текущие настройки</b>\n"]
    for loc in locs:
        city_ru = {v: k for k, v in CITIES.items()}.get(loc["city"], loc["city"])
        cat_ru = {v: k for k, v in CATEGORIES_SLUG.items()}.get(loc["category"], loc["category"])
        text_lines.append(f"• {city_ru} — {cat_ru}")

    text_lines.append(f"\n💰 Макс. цена: {config.get('max_price', 'не задана')}")
    text_lines.append(f"💰 Мин. цена: {config.get('min_price', 'не задана')}")
    text_lines.append("\nИзменить:\n/setcity <город>\n/setcategory <категория>\n/setmaxprice <сумма>\n/setminprice <сумма>")

    await message.answer("\n".join(text_lines))

@router.message(Command("setcity"), OWNER_FILTER)
async def cmd_setcity(message: Message, db: Database):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("❌ Укажите город: /setcity орша\n\nДоступные: " + ", ".join(CITIES.keys()))
        return

    city = parts[1].lower()
    city_slug = CITIES.get(city)
    if not city_slug:
        await message.answer(f"❌ Неизвестный город. Доступные: {', '.join(CITIES.keys())}")
        return

    raw_config = await db.get_setting("parser_config")
    config = ensure_config(raw_config)

    for cat_slug in set(CATEGORIES_SLUG.values()):
        new_loc = {"city": city_slug, "category": cat_slug}
        if new_loc not in config["locations"]:
            config["locations"].append(new_loc)

    await db.set_setting("parser_config", config)
    await message.answer(f"✅ Город добавлен: {city}")

@router.message(Command("setcategory"), OWNER_FILTER)
async def cmd_setcategory(message: Message, db: Database):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("❌ Укажите категорию: /setcategory компьютерная_техника\n\nДоступные: " + ", ".join(CATEGORIES_SLUG.keys()))
        return

    cat = parts[1].lower()
    cat_slug = CATEGORIES_SLUG.get(cat)
    if not cat_slug:
        await message.answer(f"❌ Неизвестная категория. Доступные: {', '.join(CATEGORIES_SLUG.keys())}")
        return

    raw_config = await db.get_setting("parser_config")
    config = ensure_config(raw_config)

    for city_slug in set(CITIES.values()):
        new_loc = {"city": city_slug, "category": cat_slug}
        if new_loc not in config["locations"]:
            config["locations"].append(new_loc)

    await db.set_setting("parser_config", config)
    await message.answer(f"✅ Категория добавлена: {cat}")

@router.message(Command("setmaxprice"), OWNER_FILTER)
async def cmd_setmaxprice(message: Message, db: Database):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("❌ Укажите сумму: /setmaxprice 5000")
        return
    try:
        price = float(parts[1])
    except ValueError:
        await message.answer("❌ Неверная сумма")
        return

    raw_config = await db.get_setting("parser_config")
    config = ensure_config(raw_config)
    config["max_price"] = price
    await db.set_setting("parser_config", config)
    await message.answer(f"✅ Максимальная цена: {price:.0f} BYN")

@router.message(Command("setminprice"), OWNER_FILTER)
async def cmd_setminprice(message: Message, db: Database):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("❌ Укажите сумму: /setminprice 50")
        return
    try:
        price = float(parts[1])
    except ValueError:
        await message.answer("❌ Неверная сумма")
        return

    raw_config = await db.get_setting("parser_config")
    config = ensure_config(raw_config)
    config["min_price"] = price
    await db.set_setting("parser_config", config)
    await message.answer(f"✅ Минимальная цена: {price:.0f} BYN")

@router.message(Command("clear"), OWNER_FILTER)
async def cmd_clear(message: Message, db: Database):
    async with aiosqlite.connect(db.db_path) as conn:
        await conn.execute("DELETE FROM products")
        await conn.execute("DELETE FROM price_alerts")
        await conn.commit()
    await message.answer("🗑️ База данных очищена.")

@router.message(F.text, OWNER_FILTER)
async def any_message(message: Message):
    await message.answer("❓ Неизвестная команда. Используйте /help для списка команд.")


# =============================================================================
# ГЛАВНЫЙ ЗАПУСК
# =============================================================================

async def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не задан!")
        return
    if OWNER_ID == 0:
        logger.error("OWNER_ID не задан!")
        return

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    db = Database()
    await db.init()

    analyzer = AIAnalyzer()
    service = BotService(bot, db, analyzer)
    await service.start()

    dp = Dispatcher()
    dp.update.middleware(DIMiddleware(service, db))
    dp.include_router(router)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        service.run_parsing_cycle,
        trigger=IntervalTrigger(minutes=20),
        id="kufar_parser",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Планировщик запущен (интервал: 20 минут)")

    existing_config = await db.get_setting("parser_config")
    if not existing_config:
        await db.set_setting("parser_config", {
            "locations": [
                {"city": "orsha", "category": "kompyuternaya-tehnika"},
                {"city": "orsha", "category": "telefony-i-planshety"},
                {"city": "orsha", "category": "chasy"},
                {"city": "baran", "category": "kompyuternaya-tehnika"},
                {"city": "baran", "category": "telefony-i-planshety"},
                {"city": "baran", "category": "chasy"},
            ],
            "max_price": 5000,
            "min_price": 50,
        })

    try:
        await bot.send_message(
            OWNER_ID,
            "🤖 <b>Kufar Resale Bot</b> запущен!\n"
            "Автопарсинг активирован (каждые 20 мин).\n"
            "Города: Орша, Барань\n"
            "Категории: компьютерная техника, телефоны и планшеты, часы\n"
            "/help — справка"
        )
    except Exception as e:
        logger.warning(f"Не удалось отправить приветствие: {e}")

    logger.info("Бот запущен. Ожидание сообщений...")
    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()
        await service.stop()
        await bot.session.close()
        logger.info("Бот остановлен.")


if __name__ == "__main__":
    asyncio.run(main())