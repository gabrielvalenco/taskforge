"""Módulo de entrada para os workers Celery::

celery -A taskforge.celery_app.worker:app worker -Q taskforge.io  -P threads -c 32
celery -A taskforge.celery_app.worker:app worker -Q taskforge.cpu -P prefork -c 4
"""

from __future__ import annotations

from taskforge.celery_app.app import create_celery_app
from taskforge.config import Settings

app = create_celery_app(Settings.from_env())
