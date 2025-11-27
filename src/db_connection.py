import os
import sys
import urllib.parse
import getpass
from sqlalchemy import create_engine


def _load_dotenv_manual(dotenv_path: str):
    """Simple .env parser: loads KEY=VALUE lines into os.environ if not already set.

    This is minimal and ignores export/complex shell constructs. It's used only as a
    fallback if `python-dotenv` isn't installed.
    """
    try:
        with open(dotenv_path, "r", encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if not ln or ln.startswith("#"):
                    continue
                if "=" not in ln:
                    continue
                k, v = ln.split("=", 1)
                k = k.strip()
                v = v.strip().strip("'\"")
                if k and (k not in os.environ):
                    os.environ[k] = v
    except FileNotFoundError:
        return


def _ensure_env_loaded():
    """Load `.env` from project root into environment if present.

    Prefer `python-dotenv` if available; otherwise use manual loader.
    """
    project_root = os.path.dirname(os.path.dirname(__file__))
    dotenv_path = os.path.join(project_root, ".env")
    if not os.path.exists(dotenv_path):
        return
    try:
        # prefer python-dotenv
        from dotenv import load_dotenv

        load_dotenv(dotenv_path)
    except Exception:
        _load_dotenv_manual(dotenv_path)


def get_engine():
    """Return a SQLAlchemy Engine.

    Resolution order:
      1. `DATABASE_URL` environment variable (recommended for production)
      2. Individual env vars: `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME`
      3. Interactive prompt for MySQL password (only when running in a TTY)
      4. Fallback to local SQLite file `data/lianes.db` (development convenience)
    """
    # Load .env into environment (if present)
    _ensure_env_loaded()

    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return create_engine(database_url)

    # Check explicit DB env vars
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT")
    dbname = os.environ.get("DB_NAME")

    if user and dbname and host:
        pwd = urllib.parse.quote_plus(password) if password else ""
        port_part = f":{port}" if port else ""
        conn = f"mysql+pymysql://{user}:{pwd}@{host}{port_part}/{dbname}"
        return create_engine(conn)

    # If running interactively, prompt for password to construct MySQL URL
    if sys.stdin.isatty():
        raw_password = getpass.getpass("Enter MySQL password (leave empty to skip): ")
        if raw_password:
            schema = os.environ.get("DB_NAME") or os.environ.get("MYSQL_DATABASE") or "lianes_library"
            host = os.environ.get("DB_HOST") or "127.0.0.1"
            user = os.environ.get("DB_USER") or os.environ.get("MYSQL_USER") or "root"
            port = os.environ.get("DB_PORT") or "3306"
            password_quoted = urllib.parse.quote_plus(raw_password)
            connection_string = f"mysql+pymysql://{user}:{password_quoted}@{host}:{port}/{schema}"
            return create_engine(connection_string)

    # Non-interactive fallback: sqlite file
    project_root = os.path.dirname(os.path.dirname(__file__))
    db_dir = os.path.join(project_root, "data")
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, "lianes.db")
    sqlite_url = f"sqlite:///{db_path}"
    try:
        import warnings

        warnings.warn(
            "DATABASE_URL not set and DB env vars not found — falling back to local sqlite at: %s" % db_path
        )
    except Exception:
        pass

    return create_engine(sqlite_url)
