FROM python:3.9

WORKDIR /bulk-upload

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

# CMD ["python", "src/insert_template_data.py"]
ENTRYPOINT ["python", "src/main.py"]