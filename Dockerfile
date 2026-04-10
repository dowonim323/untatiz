# Untatiz Docker Image
# Python 3.10 + Chromium + Supervisord for multi-process management

FROM python:3.10-slim-bookworm

LABEL maintainer="untatiz"
LABEL description="Korean Baseball Fantasy League (지재옥 리그) system"

# Prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Set timezone
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Chromium and dependencies
    chromium \
    # Display server for headless browser
    xvfb \
    # Font support for Korean
    fonts-nanum \
    fonts-nanum-coding \
    fonts-nanum-extra \
    # Build tools
    gcc \
    g++ \
    # Other utilities
    curl \
    wget \
    gnupg \
    # Supervisor for process management
    supervisor \
    # Clean up
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Chrome/Chromium environment variables
ENV CHROME_BIN=/usr/bin/chromium

# Create app directory
WORKDIR /app

# Create necessary directories
RUN mkdir -p /app/db /app/log /app/api /app/backup /app/graph /app/news /app/web/static /app/web/templates /ms-playwright

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

RUN python -m playwright install chromium

# Copy application code
COPY . .

# Copy supervisor configuration
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy entrypoint script
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create non-root user for security
RUN useradd -m -s /bin/bash untatiz && \
    chown -R untatiz:untatiz /app /ms-playwright

# Expose Flask web server port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]

# Default command: run supervisord
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
