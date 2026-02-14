FROM apache/spark:3.5.0


USER root

# Install python dependencies
COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy application code
COPY app /app
COPY data /data

WORKDIR /app

CMD ["python3", "main.py"]