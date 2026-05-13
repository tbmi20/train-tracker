import json


def load_user_settings(file_path: str) -> dict:
    """Loads user settings from a JSON file."""
    try:
        with open(file_path, "r") as f:
            settings = json.load(f)
        return settings
    except FileNotFoundError:
        print(f"Settings file not found at {file_path}. Using default settings.")
        return {}
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {file_path}. Using default settings.")
        return {}
