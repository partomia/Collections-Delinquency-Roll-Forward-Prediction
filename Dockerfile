# Streamlit app only (no torch). Runs anywhere: reads parquet exports or CDW
# Impala, and calls the CAI model endpoint for what-ifs.
#
#   docker build -t collections-app .
#   docker run -p 8501:8501 -v $PWD/data/parquet:/app/data/parquet -v $PWD/models:/app/models collections-app
#   docker run -p 8501:8501 -e COLL_STORAGE_BACKEND=impala -e COLL_IMPALA_USER=... \
#       -e COLL_IMPALA_PASSWORD=... -e COLL_ENDPOINT_URL=... collections-app
FROM python:3.11-slim

WORKDIR /app
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt

COPY coll/ coll/
COPY app/ app/
COPY config/ config/

ENV COLL_STORAGE_BACKEND=parquet \
    COLL_APP_HOST=0.0.0.0 \
    CDSW_APP_PORT=8501
EXPOSE 8501
CMD ["python", "app/run.py"]
