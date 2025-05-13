# Raffinerie IoT

Système de surveillance IoT pour raffinerie avec traitement de données en temps réel.

## Architecture

Le projet utilise les technologies suivantes :
- MQTT pour la collecte de données des capteurs
- Kafka pour le streaming de données
- Apache Spark pour le traitement en temps réel
- MinIO (S3) pour le stockage des données brutes
- TimescaleDB pour le stockage des séries temporelles
- Grafana pour la visualisation

## Structure du projet

- `simulateur_capteurs.py` : Génère des données simulées de température et vibration
- `mqtt_to_kafka.py` : Bridge entre MQTT et Kafka
- `spark/traitement_kpi.py` : Traitement Spark pour filtrer et calculer les KPIs
- `docker-compose.yml` : Configuration des services Docker

## Installation et démarrage

1. Cloner le dépôt
2. Installer les dépendances : `pip install -r requirements.txt`
3. Démarrer les services : `docker-compose up -d`
4. Lancer le bridge MQTT-Kafka : `python mqtt_to_kafka.py`
5. Lancer le simulateur : `python simulateur_capteurs.py`
6. Lancer le traitement Spark : 
   ```
   docker exec -it raffinerie-iot-spark-master-1 /opt/bitnami/spark/bin/spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4 \
     /app/traitement_kpi.py
   ```

## Accès aux interfaces

- Grafana : http://localhost:3000
- pgAdmin : http://localhost:5050
- MinIO Console : http://localhost:9001 