# One image, used by all three Python services (api, poller, worker).
# docker-compose.yml chooses which program each container runs.
FROM python:3.12-slim

# No .pyc files, and print output immediately instead of buffering it
# (otherwise `docker compose logs` can lag behind what the program is doing).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

# Copy requirements FIRST. Docker caches every step; the slow `pip install` only
# re-runs when requirements.txt changes, not every time you edit a code file.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Do not run as root inside the container.
RUN useradd --create-home appuser
USER appuser

# Default program if you run the image on its own; compose overrides it per service.
CMD ["python", "-m", "uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
