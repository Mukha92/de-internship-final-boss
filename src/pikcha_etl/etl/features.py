"""
Определения признаков для матрицы кластеризации.
Содержит метаданные о каждом признаке.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class FeatureDefinition:
    """Определение признака для витрины."""
    id: int
    name: str
    description: str
    category: str
    data_type: str = "boolean"
    
    def __post_init__(self):
        """Валидация данных признака."""
        if not self.name:
            raise ValueError("Имя признака не может быть пустым")
        if not self.description:
            raise ValueError("Описание признака не может быть пустым")


# Категории признаков
CATEGORIES = {
    "purchase_behavior": "Покупательское поведение",
    "temporal": "Временные паттерны",
    "product_preference": "Предпочтения по продуктам",
    "payment": "Способы оплаты",
    "location": "География покупок",
    "loyalty": "Лояльность",
    "spending": "Платёжеспособность",
    "basket": "Характеристики корзины"
}


# Определения всех 30 признаков
FEATURE_DEFINITIONS: list[FeatureDefinition] = [
    # Покупательское поведение
    FeatureDefinition(
        id=1, name="bought_milk_last_30d",
        description="Покупал молочные продукты за последние 30 дней",
        category="product_preference"
    ),
    FeatureDefinition(
        id=2, name="bought_fruits_last_14d",
        description="Покупал фрукты и ягоды за последние 14 дней",
        category="product_preference"
    ),
    FeatureDefinition(
        id=3, name="not_bought_veggies_14d",
        description="Не покупал овощи и зелень за последние 14 дней",
        category="product_preference"
    ),
    FeatureDefinition(
        id=4, name="recurrent_buyer",
        description="Делал более 2 покупок за последние 30 дней",
        category="purchase_behavior"
    ),
    FeatureDefinition(
        id=5, name="inactive_14_30",
        description="Не покупал 14–30 дней (ушедший клиент?)",
        category="purchase_behavior"
    ),
    FeatureDefinition(
        id=6, name="new_customer",
        description="Покупатель зарегистрировался менее 30 дней назад",
        category="loyalty"
    ),
    FeatureDefinition(
        id=7, name="delivery_user",
        description="Пользовался доставкой хотя бы раз",
        category="purchase_behavior"
    ),
    FeatureDefinition(
        id=8, name="organic_preference",
        description="Купил хотя бы 1 органический продукт",
        category="product_preference"
    ),
    FeatureDefinition(
        id=9, name="bulk_buyer",
        description="Средняя корзина > 1000₽",
        category="spending"
    ),
    FeatureDefinition(
        id=10, name="low_cost_buyer",
        description="Средняя корзина < 200₽",
        category="spending"
    ),
    FeatureDefinition(
        id=11, name="buys_bakery",
        description="Покупал хлеб/выпечку хотя бы раз",
        category="product_preference"
    ),
    FeatureDefinition(
        id=12, name="loyal_customer",
        description="Лояльный клиент (карта и ≥3 покупки)",
        category="loyalty"
    ),
    FeatureDefinition(
        id=13, name="multicity_buyer",
        description="Делал покупки в разных городах",
        category="location"
    ),
    FeatureDefinition(
        id=14, name="bought_meat_last_week",
        description="Покупал мясо/рыбу/яйца за последнюю неделю",
        category="product_preference"
    ),
    FeatureDefinition(
        id=15, name="night_shopper",
        description="Делал покупки после 20:00",
        category="temporal"
    ),
    FeatureDefinition(
        id=16, name="morning_shopper",
        description="Делал покупки до 10:00",
        category="temporal"
    ),
    FeatureDefinition(
        id=17, name="prefers_cash",
        description="Оплачивал наличными ≥ 70% покупок",
        category="payment"
    ),
    FeatureDefinition(
        id=18, name="prefers_card",
        description="Оплачивал картой ≥ 70% покупок",
        category="payment"
    ),
    FeatureDefinition(
        id=19, name="weekend_shopper",
        description="Делал ≥ 60% покупок в выходные",
        category="temporal"
    ),
    FeatureDefinition(
        id=20, name="weekday_shopper",
        description="Делал ≥ 60% покупок в будни",
        category="temporal"
    ),
    FeatureDefinition(
        id=21, name="single_item_buyer",
        description="≥50% покупок — 1 товар в корзине",
        category="basket"
    ),
    FeatureDefinition(
        id=22, name="varied_shopper",
        description="Покупал ≥4 разных категорий продуктов",
        category="basket"
    ),
    FeatureDefinition(
        id=23, name="store_loyal",
        description="Ходит только в один магазин",
        category="location"
    ),
    FeatureDefinition(
        id=24, name="switching_store",
        description="Ходит в разные магазины",
        category="location"
    ),
    FeatureDefinition(
        id=25, name="family_shopper",
        description="Среднее кол-во позиций в корзине ≥4",
        category="basket"
    ),
    FeatureDefinition(
        id=26, name="early_bird",
        description="Покупка в промежутке между 12 и 15 часами дня",
        category="temporal"
    ),
    FeatureDefinition(
        id=27, name="no_purchases",
        description="Не совершал ни одной покупки (только регистрация)",
        category="purchase_behavior"
    ),
    FeatureDefinition(
        id=28, name="recent_high_spender",
        description="Купил на сумму >2000₽ за последние 7 дней",
        category="spending"
    ),
    FeatureDefinition(
        id=29, name="fruit_lover",
        description="≥3 покупок фруктов за 30 дней",
        category="product_preference"
    ),
    FeatureDefinition(
        id=30, name="vegetarian_profile",
        description="Не купил ни одного мясного продукта за 90 дней",
        category="product_preference"
    ),
]


def get_feature_by_name(name: str) -> Optional[FeatureDefinition]:
    """Получить определение признака по имени."""
    for feature in FEATURE_DEFINITIONS:
        if feature.name == name:
            return feature
    return None


def get_features_by_category(category: str) -> list[FeatureDefinition]:
    """Получить все признаки определённой категории."""
    return [f for f in FEATURE_DEFINITIONS if f.category == category]


def get_feature_names() -> list[str]:
    """Получить список имён всех признаков."""
    return [f.name for f in FEATURE_DEFINITIONS]
