#!/usr/bin/env bash
set -euo pipefail

echo "==> Installing Docker + Git (Ubuntu)"
sudo apt-get update -y
sudo apt-get install -y git ca-certificates curl gnupg lsb-release

if ! command -v docker >/dev/null 2>&1; then
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update -y
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

sudo systemctl enable docker
sudo systemctl start docker

echo "==> Starting lab stack"
sudo docker compose up -d

echo "==> Waiting for services..."
sleep 20

echo "==> Airflow admin user (admin/admin)"
sudo docker exec -u airflow lab-airflow-web airflow db migrate || true
sudo docker exec -u airflow lab-airflow-web airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com || true

echo "==> Superset admin user (admin/admin)"
sudo docker exec lab-superset superset db upgrade || true
sudo docker exec lab-superset superset init || true
sudo docker exec lab-superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password admin || true

echo ""
echo " Ready!"
echo "phpMyAdmin: http://<EC2-IP>:8081"
echo "Airflow:    http://<EC2-IP>:8080  (admin/admin)"
echo "Superset:   http://<EC2-IP>:8088  (admin/admin)"
echo "StarRocks:  http://<EC2-IP>:8030  (root/blank)"