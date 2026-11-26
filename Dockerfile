FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    CSVJOIN_LOG_LEVEL=INFO

WORKDIR /app

# 先装依赖，利用构建缓存
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 再拷应用代码
COPY app ./app

# 非 root 运行；临时 SQLite 索引默认写 /tmp（所有作业各自 mkdtemp 子目录）
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /tmp \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status == 200 else 1)"

# 单 worker：每个请求一个独立磁盘作业目录，算子内存恒定；
# 水平扩展交给 compose replicas / 编排平台。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
