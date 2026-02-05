FROM --platform=linux/amd64 python:3.11-slim

WORKDIR /app

COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./spot_balancer

# Create non-root user and change ownership
RUN useradd -m -u 10001 -s /bin/bash appuser && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER 10001

ENV PORT=8443

# This is intended to only run on a single worker
CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:8443", "--keyfile", "/tls/tls.key", "--certfile", "/tls/tls.crt", "spot_balancer.app:app"]