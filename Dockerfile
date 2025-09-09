# syntax=docker/dockerfile:1
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Tokyo

# 基础依赖（尽量精简；如需编译可临时加 build-essential）
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata curl git \
    libffi-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 非 root 运行更安全
RUN useradd -m appuser
WORKDIR /app

# 提前复制依赖清单以利用缓存（按你项目实际情况二选一）
# 如果有 requirements.txt：
COPY requirements.txt* /app/
RUN if [ -f /app/requirements.txt ]; then pip install -r /app/requirements.txt; fi

# 如果是 pyproject.toml：
# COPY pyproject.toml poetry.lock* /app/
# RUN pip install poetry && poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

# 复制项目代码
COPY . /app

# 可选：将运行时数据目录设为可写（用于 SQLite、缓存等）
RUN mkdir -p /var/lib/copytrade && chown -R appuser:appuser /var/lib/copytrade /app
USER appuser

# 入口脚本（用环境变量决定跑哪个脚本/模块）
# 默认尝试运行 trading_strategy.py，你可以通过 APP_ENTRY/APP_ARGS 覆盖
ENV APP_ENTRY="trading_strategy.py" \
    APP_ARGS="" \
    PYTHONPATH="/app"

# 健康检查（如果你有 HTTP 健康接口可以改成 curl 检查）
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import os,sys; sys.exit(0)"

# 入口
CMD ["bash", "-lc", "python ${APP_ENTRY} ${APP_ARGS}"]
