# Search-Demand MCP connector (Wikipedia + Google Trends) — for public hosting.
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .
ENV MCP_TRANSPORT=http HOST=0.0.0.0
# No API key needed. Optional:
#   SEARCHTREND_USER_AGENT="your-app/1.0 (contact)"   (Wikimedia asks for a descriptive UA)
#   GOOGLE_TRENDS_PROXY="https://host:port"           (residential proxy for the Google Trends tool)
#   SEARCHTREND_MAX_RPS="3"
# PORT is injected by the host; the server binds to it automatically.
CMD ["python", "server.py"]
