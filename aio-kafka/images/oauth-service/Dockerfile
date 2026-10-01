FROM python:3.11.15-slim-trixie@sha256:db3ff2e1800a8581e2c48a27c3995339d47bdf046da21c7627accd3d51053a93

WORKDIR /opt/oauth

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt /opt/oauth/requirements.txt
RUN python -m pip install --disable-pip-version-check --no-cache-dir \
    --requirement /opt/oauth/requirements.txt \
    && groupadd --gid 10001 streamskope \
    && useradd --no-create-home --uid 10001 --gid 10001 streamskope
COPY server.py /opt/oauth/server.py

USER 10001:10001

EXPOSE 5000

ENTRYPOINT ["python3", "/opt/oauth/server.py"]
