# modelling-lab-stack

Browser-based Data Engineering & Data Modeling lab stack:

- MySQL + phpMyAdmin (source + UI)
- Airflow (incremental pipeline)
- StarRocks (upsert landing table)
- Superset (BI on StarRocks)

## Student setup (on EC2 Ubuntu 22.04)
```bash
sudo apt-get update -y
sudo apt-get install -y git
git clone <REPO_URL>
cd modelling-lab-stack
sudo bash scripts/bootstrap.sh

URLs
	•	phpMyAdmin: http://:8081
	•	Airflow: http://:8080
	•	Superset: http://:8088
	•	StarRocks FE: http://:8030