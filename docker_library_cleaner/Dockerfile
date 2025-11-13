FROM python:3.12.3-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT [ "python","Library_Data_Pipeline_Docker.py" ]
### To run this use docker-compose up --build ###