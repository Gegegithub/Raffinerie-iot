from kafka import KafkaConsumer
from collections import deque
import joblib
import json
import subprocess
from datetime import datetime
from dateutil import parser
import sys

print("🚀 Démarrage du script de prédiction...")

# Charger le modèle
try:
    model = joblib.load("modele_temperature.pkl")
    print("✅ Modèle chargé.")
except Exception as e:
    print(f"❌ Erreur lors du chargement du modèle : {e}")
    sys.exit(1)

# Tampon circulaire pour les 10 dernières températures
temp_buffer = deque(maxlen=10)

# Connexion à Kafka
try:
    print("🔌 Connexion à Kafka...")
    consumer = KafkaConsumer(
        "sensor-data",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True
    )
    print("✅ Connecté à Kafka.")
except Exception as e:
    print(f"❌ Erreur de connexion à Kafka : {e}")
    sys.exit(1)

# Fonction pour insérer une prédiction dans la base
def save_prediction(predicted_temp, timestamp, machine_id):
    try:
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
        INSERT INTO predictions (timestamp, machine_id, temperature_predite)
        VALUES ('{timestamp_str}', '{machine_id}', {predicted_temp});
        """
        cmd = [
            "docker", "exec", "raffinerie-iot-timescaledb-1",
            "psql", "-U", "admin", "-d", "iotdb", "-c", sql
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"📝 Prédiction enregistrée : {predicted_temp:.2f} °C")
        else:
            print(f"❌ Erreur enregistrement prédiction : {result.stderr}")
    except Exception as e:
        print(f"❌ Exception insertion prédiction : {e}")

# Boucle de prédiction en temps réel
print("🔁 Prédiction en temps réel...")
try:
    for msg in consumer:
        data = msg.value
        if data.get("type_capteur") != "temperature":
            continue

        valeur = data["valeur"]
        timestamp = parser.parse(data["timestamp"])
        machine_id = data["machine_id"]

        temp_buffer.append(valeur)
        print(f"🌡️ {timestamp} | Machine {machine_id} | Température : {valeur} °C")

        if len(temp_buffer) == 10:
            prediction = model.predict([list(temp_buffer)])[0]
            print(f"🔮 Prédiction température dans 5 pas : {prediction:.2f} °C")
            save_prediction(prediction, timestamp, machine_id)

except KeyboardInterrupt:
    print("👋 Arrêt manuel.")
    consumer.close()
except Exception as e:
    print(f"❌ Erreur : {e}")
    consumer.close()
