"""Загрузка конфигурации из YAML."""
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Создайте файл config.yaml на основе config.example.yaml в {CONFIG_PATH.parent}"
        )
    if yaml is None:
        raise ImportError("Установите pyyaml: pip install pyyaml")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
