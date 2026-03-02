FROM prefecthq/prefect:3.4.22-python3.11

WORKDIR /app

COPY orchestration/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app
RUN chmod +x config/scripts/bootstrap_prefect.sh config/scripts/run_worker.sh

ENV PREFECT_LOGGING_LEVEL=INFO
