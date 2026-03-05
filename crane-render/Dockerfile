FROM python:3.12-slim

WORKDIR /app

# Install shared lib first for layer caching
COPY crane-shared/ /deps/crane-shared/
RUN pip install --no-cache-dir /deps/crane-shared/

COPY crane-render/ /app/
RUN pip install --no-cache-dir /app/

RUN mkdir -p /data/crane-render

CMD ["crane-render"]
