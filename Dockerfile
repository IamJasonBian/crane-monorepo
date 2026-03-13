FROM python:3.12-slim

WORKDIR /opt/crane

# Install shared lib first for layer caching
COPY crane-shared/ /deps/crane-shared/
RUN pip install --no-cache-dir /deps/crane-shared/

COPY crane-feed/ /deps/crane-feed/
RUN pip install --no-cache-dir /deps/crane-feed/

CMD ["python", "-m", "crane_feed.main"]
