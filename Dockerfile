# Use multi-stage build for smaller final image
FROM python:3.11-slim AS builder

# Set environment variables to reduce size
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install only the requirements in the builder stage
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Only install Chromium browser (not Firefox or WebKit)
RUN playwright install-deps chromium && \
    playwright install --with-deps chromium

# Final stage - minimal image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    PLAYWRIGHT_BROWSERS_PATH=/usr/lib/playwright

# Install only the absolute minimal dependencies required by Chromium
# Use a single RUN command to minimize layers
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libexpat1 \
    libfontconfig1 \
    libfreetype6 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r appuser && useradd -r -g appuser -m appuser

WORKDIR /app

# Copy only the Chromium browser from builder (not the entire ms-playwright directory)
COPY --from=builder /root/.cache/ms-playwright/chromium-* ${PLAYWRIGHT_BROWSERS_PATH}/chromium-*/

# Copy Python packages - be more selective to reduce size
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/playwright /usr/local/bin/
COPY --from=builder /usr/local/bin/uvicorn /usr/local/bin/

# Copy application code
COPY ./api /app/api
COPY ./lib /app/lib
COPY ./models /app/models
COPY ./helpers.py /app/helpers.py

# Create necessary directories with a single RUN
RUN mkdir -p /app/data && \
    chown -R appuser:appuser /app ${PLAYWRIGHT_BROWSERS_PATH}

# Expose the port and switch to non-root user
EXPOSE ${PORT}
USER appuser

# Use the PORT environment variable in the command
CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${PORT}"]
