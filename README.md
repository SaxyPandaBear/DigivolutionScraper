Digivolution Scraper
====================

Automation workflow that joins known/predetermined evolution data from multiple sources with
official metadata, and then pushes that to MongoDB. This acts as a data pipeline that services
[DigimonQL](https://github.com/SaxyPandaBear/DigimonQL).

# Running

> Note: This was developed with Python 3.14.6, and Airflow 3.2.2. Deviating from this may create unexpected behavior.

Run locally via Docker:
```bash
docker compose up --build
```

Local development in virtualenv:
```bash
source bin/activate
pip install requirements.txt # TODO: finish this
```

# Testing
TBD
