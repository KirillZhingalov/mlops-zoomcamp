FROM svizor/zoomcamp-model:mlops-3.10.0-slim

# WORKDIR /app
# COPY [ "model2.bin", "model.bin" ]

COPY [ "requirements.txt", "requirements.txt" ]
RUN pip install -r requirements.txt

COPY [ "starter.py", "starter.py" ]
RUN python starter.py --year 2022 --month 4

    