FROM python:3.12-slim

WORKDIR /opt/crane

# Install shared lib first for layer caching
COPY crane-shared/ /deps/crane-shared/
RUN pip install --no-cache-dir /deps/crane-shared/

COPY crane-feed/ /deps/crane-feed/
RUN pip install --no-cache-dir /deps/crane-feed/

# Install Playwright Firefox + ALL system deps it needs, plus xvfb for headed mode
RUN python -m playwright install --with-deps firefox && \
    apt-get update && apt-get install -y --no-install-recommends xvfb xauth && \
    rm -rf /var/lib/apt/lists/*

# Ensure Playwright can find browsers regardless of user
ENV PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright

# Start xvfb in background, then run crane-feed
CMD bash -c "Xvfb :99 -screen 0 1920x1080x24 &>/dev/null & export DISPLAY=:99 && sleep 1 && python -m crane_feed.main"
