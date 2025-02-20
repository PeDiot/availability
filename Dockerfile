FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/116.0.5845.96/linux64/chromedriver-linux64.zip" \
    && unzip chromedriver-linux64.zip \
    && mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf chromedriver-linux64.zip chromedriver-linux64

COPY src/ /app/src/

COPY main.py .

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "main.py"] 