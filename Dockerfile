FROM python:3.12-slim

WORKDIR /app

# Install system deps for Firefox (Playwright) and xvfb for headed mode
RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb \
    xauth \
    libgtk-3-0 \
    libdbus-glib-1-2 \
    libxt6 \
    libx11-xcb1 \
    libasound2 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Install shared lib first for layer caching
COPY crane-shared/ /deps/crane-shared/
RUN pip install --no-cache-dir /deps/crane-shared/

COPY crane-feed/ /app/
RUN pip install --no-cache-dir /app/

# Install Playwright Firefox browser
RUN python -m playwright install firefox

# Use xvfb-run to provide a virtual display for headed Firefox
CMD ["xvfb-run", "--auto-servernum", "--server-args=-screen 0 1920x1080x24", "crane-feed"]
