FROM python:3.12-slim

WORKDIR /app

COPY crane-shared/ /deps/crane-shared/
RUN pip install --no-cache-dir /deps/crane-shared/

COPY crane-engine/ /app/
RUN pip install --no-cache-dir /app/

CMD ["crane-engine"]
