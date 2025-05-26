from kafka import KafkaConsumer
from datetime import datetime
from dateutil import parser
import json
import psutil

# Fonction pour arrêter le simulateur
def stop_simulator():
    found = False
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'simulateur_capteurs.py' in cmd:
                proc.terminate()
                print(f"🛑 Simulateur arrêté (PID: {proc.pid})")
                found = True
        except Exception:
            continue
    if not found:
        print("⚠️ Aucun processus 'simulateur_capteurs.py' trouvé.")

# Connexion à Kafka
try:
    print("🔌 Connexion à Kafka pour surveillance...")
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
    exit(1)

# Surveillance en temps réel
print("🔁 Surveillance de la température...")
try:
    for msg in consumer:
        data = msg.value
        if data.get("type_capteur") != "temperature":
            continue

        valeur = data["valeur"]
        timestamp = parser.parse(data["timestamp"])
        machine_id = data["machine_id"]

        print(f"🌡️ {timestamp} | Machine {machine_id} | Température : {valeur} °C")

        if valeur >= 150:
            print("🔥 Température critique atteinte !")
            stop_simulator()
            break  # Arrête le script après l'arrêt du simulateur

except KeyboardInterrupt:
    print("👋 Arrêt manuel.")
    consumer.close()
except Exception as e:
    print(f"❌ Erreur : {e}")
    consumer.close()
