# Use Python 3.10 as the base image
FROM python:3.10

# Install uv
RUN pip install uv

RUN mkdir -p /app && chown nobody:nogroup /app

RUN useradd -m dbtuser
RUN mkdir -p /app && chown dbtuser:dbtuser /app

WORKDIR /app

USER dbtuser

COPY --chown=dbtuser:dbtuser . /app

RUN uv sync

WORKDIR /app/dbt

CMD ["uv", "run", "dbt", "run"]