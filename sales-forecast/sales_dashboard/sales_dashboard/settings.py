"""
Django settings for the Sales Forecast dashboard project.
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# ---------------- Security (fine for local coursework use) ----------------
SECRET_KEY = "django-insecure-sales-forecast-course-project-key"
DEBUG = True
ALLOWED_HOSTS = ["*"]

# ---------------- Applications ----------------
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "dashboard",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "sales_dashboard.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "sales_dashboard.wsgi.application"

# ---------------- Database ----------------
# Using mysql-connector-python's Django backend instead of mysqlclient,
# since mysqlclient needs a C compiler on Windows and is painful to install.
# NOTE: MySQL is running on port 3307 on this machine (not the default 3306)

DATABASES = {
    "default": {
        "ENGINE": "mysql.connector.django",
        "NAME": "sales_forecast",
        "USER": "root",
        "PASSWORD": "kamana",  # <-- my password is "kamana"
        "HOST": "127.0.0.1",
        "PORT": "3307",
        "OPTIONS": {
            "autocommit": True,
        },
    }
}

# ---------------- Password validation ----------------
AUTH_PASSWORD_VALIDATORS = []  # not needed for this dashboard (no user signup)

# ---------------- Internationalization ----------------
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

# ---------------- Static files ----------------
STATIC_URL = "static/"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
