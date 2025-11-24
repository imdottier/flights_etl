FROM python:3.12

WORKDIR /app

# Install Python dependencies WITH CACHING
COPY requirements.txt /app/
RUN --mount=type=cache,id=my_shared_pip,target=/root/.cache/pip \
    pip install -r requirements.txt

# Copy app code
COPY create_postgres_db.py /app/
COPY utils /app/utils/

CMD ["python", "create_postgres_db.py"]