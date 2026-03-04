FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir .

# Expose SLURM emulator API port
EXPOSE 8080

CMD ["python3", "-m", "uvicorn", "emulator.api.emulator_server:app", "--host", "0.0.0.0", "--port", "8080"]
