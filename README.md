# modelling-lab-stack

Browser-based Data Engineering & Data Modeling Lab Stack:

- MySQL + phpMyAdmin (Source System)
- Airflow (Incremental ETL Pipeline)
- StarRocks (Upsert Landing Tables)
- Superset (BI on StarRocks)

Databases:
- MySQL DB: `order_management`
- StarRocks DB: `order_management_starrocks`

---

## Step 1 — Launch EC2

Create instance:

- Ubuntu 22.04
- Instance type: t3a.xlarge
- Storage: 100 GB gp3

### Security Group

Open the following ports:

| Port | Purpose | Source |
|------|----------|--------|
| 22   | SSH | Your IP only |
| 8081 | phpMyAdmin | 0.0.0.0/0 |
| 8080 | Airflow | 0.0.0.0/0 |
| 8088 | Superset | 0.0.0.0/0 |
| 8030 | StarRocks FE | 0.0.0.0/0 |

Do NOT open:
- 3306
- 9030
- 5432

---

## Step 2 — SSH into EC2

```bash
ssh -i yourkey.pem ubuntu@<PUBLIC_IP>

Step 3 — Install Docker
sudo apt update -y
sudo apt install -y docker.io docker-compose-plugin git
sudo systemctl enable docker
sudo systemctl start docker


Step 4 — Clone Repository
git clone <REPO_URL>
cd modelling-lab-stack
sudo docker compose up -d


Step 4 — Clone Repository
sudo docker ps
Access URLs
	•	phpMyAdmin: http://<PUBLIC_IP>:8081
	•	Airflow: http://<PUBLIC_IP>:8080
	•	Superset: http://<PUBLIC_IP>:8088
	•	StarRocks FE: http://<PUBLIC_IP>:8030
