# Chat-service
Apache Cassandra database with REST API, written in Python, using Flask framework.\
(The third project of the non-relational databases course.)

[Chat-service API](https://mif-nosql-assignments.s3.eu-central-1.amazonaws.com/2024/cassandra/cassandra-3.html) - reference point

> [!NOTE]
Project can be tested locally, running Apache Cassandra in a Docker container:\
`docker run -p 9042:9042 -d cassandra`

> [!TIP]
Use commands below to:
> - create & activate Python virtual environment;
> - install the requirements;
> - run the Flask app;
> - perform automatic tests.

For windows (powershell):
```powershell
python -m venv .venv
.venv\Scripts\activate.ps1 
pip install -r requirements.txt
.\run.bat
pip install -r test\requirements.txt
pytest test\test_api.py
```

For linux/mac:
```shell
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./run.sh
pip install -r test/requirements.txt
pytest test/test_api.py
```
> [!TIP]
Manual testing can be performed using Postman: `http://localhost:8080/<the rest of URL>`.\
> (JSON can be loaded by: `Body – raw – JSON`.)
