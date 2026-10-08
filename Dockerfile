FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends bash git ca-certificates ripgrep coreutils \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 agent \
    && mkdir /workspace && chown agent:agent /workspace
COPY helper.py /usr/local/lib/xgovernor/helper.py
RUN chmod 0555 /usr/local/lib/xgovernor/helper.py
USER 10001:10001
WORKDIR /workspace
ENV HOME=/home/agent
CMD ["sleep", "infinity"]
