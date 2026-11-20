FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.lock ./
RUN pip install -r requirements.lock && groupadd -g 10001 app && useradd -u 10001 -g app app
COPY --chown=app:app . .
RUN mkdir -p /app/data /app/staticfiles && chown -R app:app /app/data /app/staticfiles
USER app
RUN DEMO_MODE=true DEBUG=false python manage.py collectstatic --noinput
EXPOSE 8000
CMD ["gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "2", "--timeout", "120", "--access-logfile", "-"]
