import os

env = os.environ.get

MYSQL_HOST = env("MYSQL_HOST", "139.159.176.118")

MYSQL_PORT = int(env("MYSQL_PORT", 3306))

MYSQL_USER = env("MYSQL_USER", "dcr")

MYSQL_PASSWORD = env("MYSQL_PASSWORD", "")

MYSQL_DB = env("MYSQL_DB", "datacenter")

MONGO_URL = env("MONGO_URL", "mongodb://127.0.0.1:27137")

MONGO_URL_JQData = env("MONGO_URL_JQData", "mongodb://127.0.0.1:27137")

MONGO_URL_STOCK = env("MONGO_URL_STOCK", "mongodb://127.0.0.1:27137")

MONGO_DB1 = env("MONGO_DB", "JQdata")

MONGO_DB2 = env("MONGO_DB", "stock")

MONGO_COLL_CALENDARS = env("MONGO_COLL", "calendars")

MONGO_COLL_INDEX = env("MONGO_COLL_INDEX", "generate_indexcomponentsweight")

SENTRY_DSN = env("SENTRY_DSN", "https://330e494ccd22497db605a102491c0423@sentry.io/1501024")
