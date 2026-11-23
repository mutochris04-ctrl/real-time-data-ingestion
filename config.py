"""Settings for the Spark job, read from the .env file (so no password is written in the code)."""
import os


def read_env_file(file_name=".env"):
    """Read lines like PG_PORT=5432 from the .env file and keep them in a dictionary."""
    values = {}
    if not os.path.exists(file_name):
        print("Warning:", file_name, "not found - copy .env.example to .env")
        return values
    with open(file_name) as f:
        for line in f:
            line = line.strip()
            if line == "" or line.startswith("#"):
                continue                      # skip empty lines and comments
            name, value = line.split("=", 1)
            values[name] = value
    return values


settings = read_env_file()

DB_HOST = settings.get("PG_HOST", "localhost")
DB_PORT = int(settings.get("PG_PORT", "5432"))
DB_NAME = settings.get("PG_DATABASE", "ecommerce_events")
DB_USER = settings.get("PG_USER", "postgres")
DB_PASSWORD = settings.get("PG_PASSWORD", "")
