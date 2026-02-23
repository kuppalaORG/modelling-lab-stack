# modelling-lab-stack

Browser-based Data Engineering & Data Modeling lab stack:

- MySQL + phpMyAdmin (source + UI)
- Airflow (incremental pipeline)
- StarRocks (upsert landing table)
- Superset (BI on StarRocks)
Step 1 — Launch EC2

Create:
	•	Ubuntu 22.04
	•	t3a.xlarge
	•	100 GB gp3
	•	Security Group:

Open:
	•	22 (your IP only)
	•	8081
	•	8080
	•	8088
	•	8030
Step 2 — SSH into EC2
ssh -i yourkey.pem ubuntu@<PUBLIC_IP>

Step 3 — Install Docker
sudo apt update -y
sudo apt install -y docker.io docker-compose-plugin git
sudo systemctl enable docker
sudo systemctl start docker

Step 4 — Clone your repo
git clone <REPO_URL>
cd modelling-lab-stack
sudo docker compose up -d
Wait 1–2 minutes.

Check containers:
    sudo docker ps

URLs
	•	phpMyAdmin: http://:8081
	•	Airflow: http://:8080
	•	Superset: http://:8088
	•	StarRocks FE: http://:8030