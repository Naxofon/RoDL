FROM python:3.11-slim

WORKDIR /workspace

COPY admin_bot/requirements.txt /workspace/prefect_loader/admin_bot/requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --default-timeout=15 --retries 3 \
    -r /workspace/prefect_loader/admin_bot/requirements.txt

COPY . /workspace/prefect_loader

ENV PYTHONPATH=/workspace

WORKDIR /workspace/prefect_loader

CMD ["bash", "config/scripts/run_bot.sh"]
