"""ETL-процессы для витрин данных."""

from src.pikcha_etl.etl.process import CustomerFeatureETL
from src.pikcha_etl.etl.features import (
    FeatureDefinition,
    FEATURE_DEFINITIONS,
    CATEGORIES,
    get_feature_by_name,
    get_features_by_category,
    get_feature_names,
)

__all__ = [
    "CustomerFeatureETL",
    "FeatureDefinition",
    "FEATURE_DEFINITIONS",
    "CATEGORIES",
    "get_feature_by_name",
    "get_features_by_category",
    "get_feature_names",
]
