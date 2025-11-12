FROM python:3.12.3-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirments.txt
ENTRYPOINT [ "python","calculator_app.py" ]