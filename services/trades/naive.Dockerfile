# Use Python 3.11 slim image
FROM python:3.11-slim-bookworm

# Install UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# # Set environment variables
# ENV PYTHONUNBUFFERED=1 \
#     PATH="/bin:${PATH}"

# Set working directory in the container
WORKDIR /app

#Copy the project into the image
ADD . /app

# Create virtual environment
RUN uv venv

# Install dependencies
RUN uv sync --frozen

# Run the application
CMD ["uv", "run",  "run.py"]
