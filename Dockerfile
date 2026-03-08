FROM python:3.12-slim

WORKDIR /app

# Install shared lib first for layer caching
COPY crane-shared/ /deps/crane-shared/
RUN pip install --no-cache-dir /deps/crane-shared/

COPY crane-feed/ /app/
RUN pip install --no-cache-dir /app/

# Install Playwright Firefox + ALL system deps it needs, plus xvfb for headed mode
RUN python -m playwright install --with-deps firefox && \
    apt-get update && apt-get install -y --no-install-recommends xvfb xauth && \
    rm -rf /var/lib/apt/lists/*

# Use xvfb-run to provide a virtual display for headed Firefox
CMD ["xvfb-run", "--auto-servernum", "--server-args=-screen 0 1920x1080x24", "crane-feed"]
