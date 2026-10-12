FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY isu_parser /app/isu_parser
COPY skating_api /app/skating_api
# Edited 2026-10-12 by Claude (AI assistant): the parsers read the per-source profiles (club/nation columns) during imports
COPY config/source_profiles.json /app/config/source_profiles.json

# Edited 2026-10-12 by Claude (AI assistant): [pdf] = judges-score PDFs (element scores + judge marks) during imports
RUN pip install --no-cache-dir ".[pdf]"

EXPOSE 8000

CMD ["uvicorn", "skating_api.app:app", "--host", "0.0.0.0", "--port", "8000"]
