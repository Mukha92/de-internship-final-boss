"""Генератор синтетических данных для продуктового ритейла."""

from __future__ import annotations

import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Добавляем корень проекта в PYTHONPATH для запуска как скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from faker import Faker

from config import get_settings
from src.pikcha_etl.types import JSONList


class GroceryDataGenerator:
    """
    Класс для генерации тестовых данных продуктового магазина:
    магазины, товары, покупатели и истории покупок.
    """
    
    # Константы категорий
    CATEGORIES = [
        "🥖 Зерновые и хлебобулочные изделия",
        "🥩 Мясо, рыба, яйца и бобовые",
        "🥛 Молочные продукты",
        "🍏 Фрукты и ягоды",
        "🥦 Овощи и зелень"
    ]

    STORE_NETWORKS = [("Большая Пикча", 30), ("Маленькая Пикча", 15)]

    REAL_CITIES = [
        {"name": "Москва", "lat": 55.7558, "lon": 37.6176},
        {"name": "Санкт-Петербург", "lat": 59.9343, "lon": 30.3351},
        {"name": "Новосибирск", "lat": 55.0302, "lon": 82.9204},
        {"name": "Екатеринбург", "lat": 56.8389, "lon": 60.6057},
        {"name": "Казань", "lat": 55.7961, "lon": 49.1064},
        {"name": "Нижний Новгород", "lat": 56.3249, "lon": 44.0059},
        {"name": "Челябинск", "lat": 55.1644, "lon": 61.4368},
        {"name": "Самара", "lat": 53.2415, "lon": 50.2212},
        {"name": "Омск", "lat": 54.9893, "lon": 73.3682},
        {"name": "Ростов-на-Дону", "lat": 47.2357, "lon": 39.7015},
        {"name": "Уфа", "lat": 54.7388, "lon": 55.9721},
        {"name": "Красноярск", "lat": 56.0106, "lon": 92.8525},
        {"name": "Воронеж", "lat": 51.6606, "lon": 39.1976},
        {"name": "Пермь", "lat": 58.0105, "lon": 56.2502},
        {"name": "Волгоград", "lat": 48.7071, "lon": 44.5169},
        {"name": "Краснодар", "lat": 45.0355, "lon": 38.9753},
        {"name": "Саратов", "lat": 51.5336, "lon": 46.0343},
        {"name": "Тюмень", "lat": 57.1535, "lon": 65.5343},
        {"name": "Тольятти", "lat": 53.5078, "lon": 49.4204},
        {"name": "Ижевск", "lat": 56.8528, "lon": 53.2116},
    ]

    # Список товаров (сокращённый)
    REAL_PRODUCTS = [
        # Категория 0: Хлебобулочные
        {"name": "Хлеб Бородинский", "group": CATEGORIES[0], "description": "Ржаной хлеб с кориандром", "price": 55.0, "unit": "шт", "manufacturer_name": "ООО «Коломенский пекарь»"},
        {"name": "Батон нарезной", "group": CATEGORIES[0], "description": "Сдобный батон", "price": 42.0, "unit": "шт", "manufacturer_name": "АО «Каравай»"},
        {"name": "Лаваш тонкий", "group": CATEGORIES[0], "description": "Армянский лаваш, 300 г", "price": 60.0, "unit": "шт", "manufacturer_name": "ООО «Кавказская кухня»"},
        {"name": "Гречка", "group": CATEGORIES[0], "description": "Крупа гречневая, 900 г", "price": 75.0, "unit": "шт", "manufacturer_name": "ООО «Мистраль»"},
        {"name": "Рис круглозёрный", "group": CATEGORIES[0], "description": "Рис для плова, 900 г", "price": 80.0, "unit": "шт", "manufacturer_name": "АО «Агроимпорт»"},
        {"name": "Овсяные хлопья", "group": CATEGORIES[0], "description": "Геркулес, 500 г", "price": 45.0, "unit": "шт", "manufacturer_name": "ООО «Овсянка»"},
        {"name": "Макароны спагетти", "group": CATEGORIES[0], "description": "Из твёрдых сортов пшеницы", "price": 60.0, "unit": "шт", "manufacturer_name": "ООО «Макфа»"},
        {"name": "Мука пшеничная", "group": CATEGORIES[0], "description": "Для выпечки, 2 кг", "price": 85.0, "unit": "шт", "manufacturer_name": "ООО «Мельница»"},
        {"name": "Печенье сахарное", "group": CATEGORIES[0], "description": "К чаю, 350 г", "price": 70.0, "unit": "шт", "manufacturer_name": "ООО «Кондитер»"},
        {"name": "Кукурузные хлопья", "group": CATEGORIES[0], "description": "Готовый завтрак", "price": 95.0, "unit": "шт", "manufacturer_name": "ООО «Любятово»"},
        # Категория 1: Мясо, рыба, яйца
        {"name": "Филе куриное", "group": CATEGORIES[1], "description": "Охлаждённое", "price": 320.0, "unit": "кг", "manufacturer_name": "Птицефабрика «Северная»"},
        {"name": "Говядина тушёная", "group": CATEGORIES[1], "description": "Консервы, 325 г", "price": 120.0, "unit": "шт", "manufacturer_name": "ООО «Мясной двор»"},
        {"name": "Яйцо куриное С1", "group": CATEGORIES[1], "description": "10 шт", "price": 85.0, "unit": "уп", "manufacturer_name": "Птицефабрика «Роскар»"},
        {"name": "Лосось слабосолёный", "group": CATEGORIES[1], "description": "Филе, 200 г", "price": 350.0, "unit": "шт", "manufacturer_name": "Русское море"},
        {"name": "Фасоль красная", "group": CATEGORIES[1], "description": "В собственном соку, 400 г", "price": 90.0, "unit": "шт", "manufacturer_name": "Бондюэль"},
        {"name": "Свинина шейка", "group": CATEGORIES[1], "description": "Охлаждённая", "price": 450.0, "unit": "кг", "manufacturer_name": "Мираторг"},
        {"name": "Пельмени русские", "group": CATEGORIES[1], "description": "Замороженные, 500 г", "price": 130.0, "unit": "уп", "manufacturer_name": "ООО «Сибирский гурман»"},
        {"name": "Сосиски молочные", "group": CATEGORIES[1], "description": "Варёные, 400 г", "price": 140.0, "unit": "шт", "manufacturer_name": "ООО «Велком»"},
        {"name": "Горох колотый", "group": CATEGORIES[1], "description": "Сухой, 800 г", "price": 45.0, "unit": "шт", "manufacturer_name": "ООО «Крупяной двор»"},
        {"name": "Чечевица зелёная", "group": CATEGORIES[1], "description": "Сухая, 500 г", "price": 70.0, "unit": "шт", "manufacturer_name": "ООО «Здоровое зерно»"},
        # Категория 2: Молочные
        {"name": "Молоко 3,2%", "group": CATEGORIES[2], "description": "Пастеризованное, 1 л", "price": 75.0, "unit": "шт", "manufacturer_name": "Вимм-Билль-Данн"},
        {"name": "Творог 5%", "group": CATEGORIES[2], "description": "Мягкий, 200 г", "price": 95.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
        {"name": "Сыр Российский", "group": CATEGORIES[2], "description": "45% жирности, 300 г", "price": 200.0, "unit": "шт", "manufacturer_name": "Сыробогатов"},
        {"name": "Йогурт клубничный", "group": CATEGORIES[2], "description": "1,5%, 270 г", "price": 48.0, "unit": "шт", "manufacturer_name": "Danone"},
        {"name": "Кефир 2,5%", "group": CATEGORIES[2], "description": "1 л", "price": 70.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
        {"name": "Сметана 20%", "group": CATEGORIES[2], "description": "300 г", "price": 80.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
        {"name": "Масло сливочное", "group": CATEGORIES[2], "description": "82,5%, 180 г", "price": 120.0, "unit": "шт", "manufacturer_name": "ООО «Маслозавод»"},
        {"name": "Сыр Моцарелла", "group": CATEGORIES[2], "description": "Для пиццы, 125 г", "price": 100.0, "unit": "шт", "manufacturer_name": "ООО «Италика»"},
        {"name": "Мороженое пломбир", "group": CATEGORIES[2], "description": "Ванильное, 500 г", "price": 130.0, "unit": "шт", "manufacturer_name": "ООО «Айсберг»"},
        {"name": "Ряженка", "group": CATEGORIES[2], "description": "Топлёное молоко, 500 мл", "price": 55.0, "unit": "шт", "manufacturer_name": "Вимм-Билль-Данн"},
        # Категория 3: Фрукты и ягоды
        {"name": "Яблоки Голден", "group": CATEGORIES[3], "description": "Сладкие, 1 кг", "price": 110.0, "unit": "кг", "manufacturer_name": "ИП Фруктовый сад"},
        {"name": "Бананы", "group": CATEGORIES[3], "description": "Эквадор, 1 кг", "price": 95.0, "unit": "кг", "manufacturer_name": "Эко-Фрукты"},
        {"name": "Апельсины", "group": CATEGORIES[3], "description": "Сочные, 1 кг", "price": 120.0, "unit": "кг", "manufacturer_name": "Средиземноморский экспорт"},
        {"name": "Лимоны", "group": CATEGORIES[3], "description": "1 кг", "price": 80.0, "unit": "кг", "manufacturer_name": "ИП Цитрус"},
        {"name": "Груши Конференция", "group": CATEGORIES[3], "description": "Сочные, 1 кг", "price": 150.0, "unit": "кг", "manufacturer_name": "ИП Фруктовый сад"},
        {"name": "Мандарины", "group": CATEGORIES[3], "description": "Абхазские, 1 кг", "price": 130.0, "unit": "кг", "manufacturer_name": "Кавказские фрукты"},
        {"name": "Киви", "group": CATEGORIES[3], "description": "Зелёный, 1 кг", "price": 200.0, "unit": "кг", "manufacturer_name": "ИП Фруктовый сад"},
        {"name": "Клубника", "group": CATEGORIES[3], "description": "Свежая, 500 г", "price": 150.0, "unit": "шт", "manufacturer_name": "ИП Ягодная поляна"},
        {"name": "Виноград кишмиш", "group": CATEGORIES[3], "description": "Без косточек, 1 кг", "price": 160.0, "unit": "кг", "manufacturer_name": "Азербайджанские фрукты"},
        {"name": "Арбуз", "group": CATEGORIES[3], "description": "Свежий, ≈5 кг", "price": 250.0, "unit": "шт", "manufacturer_name": "Астраханские арбузы"},
        # Категория 4: Овощи и зелень
        {"name": "Картофель мытый", "group": CATEGORIES[4], "description": "Молодой, 1 кг", "price": 40.0, "unit": "кг", "manufacturer_name": "Фермерское хозяйство «Лето»"},
        {"name": "Огурцы", "group": CATEGORIES[4], "description": "Тепличные, 1 кг", "price": 130.0, "unit": "кг", "manufacturer_name": "Агрокультура"},
        {"name": "Помидоры черри", "group": CATEGORIES[4], "description": "На ветке, 250 г", "price": 85.0, "unit": "уп", "manufacturer_name": "Белая дача"},
        {"name": "Морковь мытая", "group": CATEGORIES[4], "description": "Столовая, 1 кг", "price": 45.0, "unit": "кг", "manufacturer_name": "Фермерское хозяйство «Лето»"},
        {"name": "Лук репчатый", "group": CATEGORIES[4], "description": "1 кг", "price": 30.0, "unit": "кг", "manufacturer_name": "Фермерское хозяйство «Лето»"},
        {"name": "Капуста", "group": CATEGORIES[4], "description": "Белокочанная, 1 кг", "price": 25.0, "unit": "кг", "manufacturer_name": "Агрокультура"},
        {"name": "Перец болгарский", "group": CATEGORIES[4], "description": "Красный, 1 кг", "price": 150.0, "unit": "кг", "manufacturer_name": "Агрокультура"},
        {"name": "Укроп свежий", "group": CATEGORIES[4], "description": "Пучок", "price": 20.0, "unit": "шт", "manufacturer_name": "ИП «Зелень»"},
        {"name": "Чеснок", "group": CATEGORIES[4], "description": "Головки, 1 кг", "price": 150.0, "unit": "кг", "manufacturer_name": "ИП «Огородник»"},
        {"name": "Шампиньоны", "group": CATEGORIES[4], "description": "Свежие, 400 г", "price": 90.0, "unit": "шт", "manufacturer_name": "ООО «Грибная ферма»"},
    ]

    def __init__(self, output_dir: Optional[str] = None):
        """
        Инициализация генератора.
        
        Args:
            output_dir: Директория для сохранения данных
        """
        settings = get_settings()
        self.fake = Faker("ru_RU")
        self.output_dir = output_dir or settings.data_dir

    def _generate_kbju(self, group: str) -> dict:
        """Генерация КБЖУ на основе категории."""
        if group == self.CATEGORIES[0]:  # Хлебобулочные
            return {
                "calories": round(random.uniform(200, 350), 1),
                "protein": round(random.uniform(6, 12), 1),
                "fat": round(random.uniform(1, 6), 1),
                "carbohydrates": round(random.uniform(40, 60), 1)
            }
        elif group == self.CATEGORIES[1]:  # Мясо, рыба
            return {
                "calories": round(random.uniform(120, 350), 1),
                "protein": round(random.uniform(15, 30), 1),
                "fat": round(random.uniform(2, 25), 1),
                "carbohydrates": round(random.uniform(0, 15), 1)
            }
        elif group == self.CATEGORIES[2]:  # Молочные
            return {
                "calories": round(random.uniform(40, 200), 1),
                "protein": round(random.uniform(3, 10), 1),
                "fat": round(random.uniform(1, 15), 1),
                "carbohydrates": round(random.uniform(3, 12), 1)
            }
        elif group == self.CATEGORIES[3]:  # Фрукты
            return {
                "calories": round(random.uniform(30, 100), 1),
                "protein": round(random.uniform(0.5, 2), 1),
                "fat": round(random.uniform(0.1, 0.8), 1),
                "carbohydrates": round(random.uniform(5, 20), 1)
            }
        else:  # Овощи
            return {
                "calories": round(random.uniform(15, 60), 1),
                "protein": round(random.uniform(0.5, 3), 1),
                "fat": round(random.uniform(0.1, 0.8), 1),
                "carbohydrates": round(random.uniform(2, 10), 1)
            }

    def _create_directories(self) -> None:
        """Создаёт необходимые директории для данных."""
        dirs = ["stores", "products", "customers", "purchases"]
        for d in dirs:
            os.makedirs(os.path.join(self.output_dir, d), exist_ok=True)

    def generate_stores(self, num_stores: Optional[int] = None) -> JSONList:
        """
        Генерирует магазины на основе сетей и сохраняет в JSON.

        Args:
            num_stores: Общее количество магазинов. Если None, используется
                        распределение из STORE_NETWORKS (30 + 15 = 45)
        """
        stores = []
        store_counter = 1

        # Если задано конкретное количество, распределяем поровну между сетями
        if num_stores is not None:
            stores_per_network = num_stores // len(self.STORE_NETWORKS)
            remainder = num_stores % len(self.STORE_NETWORKS)
            network_counts = [
                stores_per_network + (1 if i < remainder else 0)
                for i in range(len(self.STORE_NETWORKS))
            ]
        else:
            # Используем распределение по умолчанию из STORE_NETWORKS
            network_counts = [count for _, count in self.STORE_NETWORKS]

        for idx, (network, _) in enumerate(self.STORE_NETWORKS):
            for _ in range(network_counts[idx]):
                store_id = f"store-{store_counter:03d}"
                city_info = random.choice(self.REAL_CITIES)
                city = city_info["name"]
                latitude = city_info["lat"]
                longitude = city_info["lon"]
                lat_offset = random.uniform(-0.01, 0.01)
                lon_offset = random.uniform(-0.01, 0.01)
                street = self.fake.street_name()

                store = {
                    "store_id": store_id,
                    "store_name": f"{network} — Магазин на {street}",
                    "store_network": network,
                    "store_type_description": f"{'Супермаркет более 200 кв.м.' if network == 'Большая Пикча' else 'Магазин у дома менее 100 кв.м.'} Входит в сеть из {network_counts[idx]} магазинов.",
                    "type": "offline",
                    "categories": self.CATEGORIES,
                    "manager": {
                        "name": self.fake.name(),
                        "phone": self.fake.phone_number(),
                        "email": self.fake.email()
                    },
                    "location": {
                        "country": "Россия",
                        "city": city,
                        "street": street,
                        "house": str(self.fake.building_number()),
                        "postal_code": self.fake.postcode(),
                        "coordinates": {
                            "latitude": round(latitude + lat_offset, 6),
                            "longitude": round(longitude + lon_offset, 6)
                        }
                    },
                    "opening_hours": {
                        "mon_fri": "09:00-21:00",
                        "sat": "10:00-20:00",
                        "sun": "10:00-18:00"
                    },
                    "accepts_online_orders": True,
                    "delivery_available": True,
                    "warehouse_connected": random.choice([True, False]),
                    "last_inventory_date": datetime.now().strftime("%Y-%m-%d")
                }
                stores.append(store)
                path = os.path.join(self.output_dir, "stores", f"{store_id}.json")
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(store, f, ensure_ascii=False, indent=2)
                store_counter += 1
                
        print(f"Сгенерировано {len(stores)} магазинов")
        return stores

    def generate_products(self, num_products: Optional[int] = None) -> JSONList:
        """
        Выбирает товары из каждой категории и генерирует их с деталями.

        Args:
            num_products: Общее количество товаров. Если None, выбираются
                          все 50 товаров (по 10 из каждой категории)
        """
        selected_products = []

        # Если задано конкретное количество, распределяем по категориям
        if num_products is not None:
            products_per_category = num_products // len(self.CATEGORIES)
            remainder = num_products % len(self.CATEGORIES)
        else:
            products_per_category = 10
            remainder = 0

        for idx, category in enumerate(self.CATEGORIES):
            cat_products = [p for p in self.REAL_PRODUCTS if p["group"] == category]
            random.shuffle(cat_products)
            # Берём нужное количество товаров из категории
            count = products_per_category + (1 if idx < remainder else 0)
            selected_products.extend(cat_products[:count])

        products = []
        
        for i, real_prod in enumerate(selected_products):
            product_id = f"prd-{1000 + i}"

            manufacturer_name = real_prod["manufacturer_name"]
            website_base = manufacturer_name.lower().replace(' ', '').replace('«', '').replace('»', '').replace('ооо', '').replace('ао', '').replace('ип', '')
            website_domain = (website_base[:30] + ".ru") if website_base else self.fake.domain_name()

            product = {
                "id": product_id,
                "name": real_prod["name"],
                "group": real_prod["group"],
                "description": real_prod["description"],
                "kbju": self._generate_kbju(real_prod["group"]),
                "price": real_prod["price"],
                "unit": real_prod["unit"],
                "origin_country": "Россия",
                "expiry_days": random.randint(5, 30),
                "is_organic": random.choice([True, False]),
                "barcode": self.fake.ean(length=13),
                "manufacturer": {
                    "name": manufacturer_name,
                    "country": "Россия",
                    "website": f"https://{website_domain}",
                    "inn": self.fake.bothify(text='##########')
                }
            }
            products.append(product)
            path = os.path.join(self.output_dir, "products", f"{product['id']}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(product, f, ensure_ascii=False, indent=2)

        print(f"Сгенерировано {len(products)} товаров")
        return products

    def generate_customers(
        self,
        stores: JSONList,
        num_customers: Optional[int] = None,
    ) -> JSONList:
        """
        Генерирует покупателей, привязанных к магазинам.

        Args:
            stores: Список магазинов для привязки покупателей
            num_customers: Количество покупателей. Если None, создаётся
                           один покупатель на каждый магазин
        """
        customers = []

        # Определяем количество покупателей
        if num_customers is not None:
            customer_count = num_customers
        else:
            customer_count = len(stores)

        for i in range(customer_count):
            # Если покупателей больше чем магазинов, циклически используем магазины
            store = stores[i % len(stores)]
            customer_id = f"cus-{1000 + len(customers)}"
            gender = random.choice(["male", "female"])
            
            if gender == "male":
                first_name = self.fake.first_name_male()
                last_name = self.fake.last_name_male()
            else:
                first_name = self.fake.first_name_female()
                last_name = self.fake.last_name_female()

            # Генерируем дату регистрации: 50% клиентов > 30 дней назад, 50% <= 30 дней
            if random.random() < 0.5:
                # "Новые" клиенты: регистрация от 1 до 29 дней назад
                days_ago = random.randint(1, 29)
            else:
                # "Старые" клиенты: регистрация от 31 до 365 дней назад
                days_ago = random.randint(31, 365)
            
            registration_date = datetime.now() - timedelta(days=days_ago)

            customer = {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": self.fake.email(),
                "phone": self.fake.phone_number(),
                "birth_date": self.fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
                "gender": gender,
                "registration_date": registration_date.isoformat(),
                "is_loyalty_member": True,
                "loyalty_card_number": f"LOYAL-{uuid.uuid4().hex[:10].upper()}",
                "purchase_location": store["location"],
                "delivery_address": {
                    "country": "Россия",
                    "city": store["location"]["city"],
                    "street": self.fake.street_name(),
                    "house": str(self.fake.building_number()),
                    "apartment": str(random.randint(1, 100)),
                    "postal_code": self.fake.postcode()
                },
                "preferences": {
                    "preferred_language": "ru",
                    "preferred_payment_method": random.choice(["card", "cash"]),
                    "receive_promotions": random.choice([True, False])
                }
            }
            customers.append(customer)
            path = os.path.join(self.output_dir, "customers", f"{customer_id}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(customer, f, ensure_ascii=False, indent=2)

        print(f"Сгенерировано {len(customers)} покупателей")
        return customers

    def generate_purchases(
        self,
        customers: JSONList,
        stores: JSONList,
        products: JSONList,
        num_purchases: int = 200,
    ) -> JSONList:
        """Генерирует заданное количество покупок и сохраняет в JSON."""
        purchases = []
        
        for i in range(num_purchases):
            customer = random.choice(customers)
            store = random.choice(stores)
            items = random.sample(products, k=random.randint(1, 3))
            purchase_items = []
            total = 0
            
            for item in items:
                qty = random.randint(1, 5)
                total_price = round(item["price"] * qty, 2)
                total += total_price
                purchase_items.append({
                    "product_id": item["id"],
                    "name": item["name"],
                    "category": item["group"],
                    "quantity": qty,
                    "unit": item["unit"],
                    "price_per_unit": item["price"],
                    "total_price": total_price,
                    "kbju": item["kbju"],
                    "manufacturer": item["manufacturer"]
                })
                
            purchase = {
                "purchase_id": f"ord-{i+1:05d}",
                "customer": {
                    "customer_id": customer["customer_id"],
                    "first_name": customer["first_name"],
                    "last_name": customer["last_name"]
                },
                "store": {
                    "store_id": store["store_id"],
                    "store_name": store["store_name"],
                    "store_network": store["store_network"],
                    "location": store["location"]
                },
                "items": purchase_items,
                "total_amount": round(total, 2),
                "payment_method": random.choice(["card", "cash"]),
                "is_delivery": random.choice([True, False]),
                "delivery_address": customer["delivery_address"],
                "purchase_datetime": (datetime.now() - timedelta(days=random.randint(0, 90))).isoformat()
            }
            purchases.append(purchase)
            path = os.path.join(self.output_dir, "purchases", f"{purchase['purchase_id']}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(purchase, f, ensure_ascii=False, indent=2)

        print(f"Сгенерировано {len(purchases)} покупок")
        return purchases

    def run(
        self,
        num_stores: Optional[int] = None,
        num_products: Optional[int] = None,
        num_customers: Optional[int] = None,
        num_purchases: int = 200,
    ) -> dict[str, int]:
        """
        Главная функция, запускающая полную генерацию данных.

        Args:
            num_stores: Количество магазинов. По умолчанию 45 (30 + 15 по сетям)
            num_products: Количество товаров. По умолчанию 50 (по 10 из категории)
            num_customers: Количество покупателей. По умолчанию = количеству магазинов
            num_purchases: Количество покупок. По умолчанию 200

        Returns:
            Словарь с количеством сгенерированных объектов по типам
        """
        self._create_directories()
        stores = self.generate_stores(num_stores=num_stores)
        products = self.generate_products(num_products=num_products)
        customers = self.generate_customers(stores, num_customers=num_customers)
        purchases = self.generate_purchases(
            customers, stores, products, num_purchases=num_purchases
        )
        print("Генерация данных завершена!")
        return {
            "stores": len(stores),
            "products": len(products),
            "customers": len(customers),
            "purchases": len(purchases),
        }
