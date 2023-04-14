FROM python:3.10-buster

RUN pip install datauri==1.0.0 aiohttp==3.8.4

WORKDIR crawler

COPY main.py .

CMD ["python", "main.py"]