import os


def get_settings():
    return {
        "environment": os.getenv("ENVIRONMENT", "local"),
        "database_url": os.getenv("DATABASE_URL", ""),
    }
