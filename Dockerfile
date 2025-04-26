# Use multi-stage build for smaller final image
FROM python:3.11-slim AS builder

# Set environment variables to reduce size and improve security
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install only the dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    playwright install-deps && \
    playwright install --with-deps chromium

# Final stage - copy only what's needed
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy installed dependencies from builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Copy necessary browsers from builder stage
COPY --from=builder /root/.cache/ms-playwright /root/.cache/ms-playwright

# Copy application code
COPY ./api /app/api
COPY ./lib /app/lib
COPY ./models /app/models
COPY ./helpers.py /app/helpers.py

# Create necessary directories with a single RUN to reduce layers
RUN mkdir -p /app/data

# Specify the command to run with a port above 1024 (non-privileged port)
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]