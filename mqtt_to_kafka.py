import paho.mqtt.client as mqtt 
from kafka import KafkaProducer 
from kafka.admin import KafkaAdminClient, NewTopic 
import json 
KAFKA_BROKER = "localhost:9092" 
TOPIC_NAME = "sensor-data" 
# 1. Créer le topic s'il n'existe pas déjà 
try: 
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER) 
    topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=1, 
    replication_factor=1)] 
    admin_client.create_topics(new_topics=topic_list, validate_only=False) 
    print(f"✅ Topic '{TOPIC_NAME}' créé.") 
except Exception as e: 
    print(f"ℹ️ Topic existe déjà ou erreur ignorée : {e}") 

# 2. Initialiser le producteur Kafka 
try:
    producer = KafkaProducer( 
        bootstrap_servers=KAFKA_BROKER, 
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    print("✅ Connexion au broker Kafka établie.")
except Exception as e:
    print(f"❌ Erreur de connexion au broker Kafka : {e}")
    exit(1)
    
# 3. Fonction callback quand MQTT reçoit un message 
def on_message(client, userdata, msg): 
    print(f"📥 [MQTT] Reçu sur {msg.topic} → {msg.payload.decode()}") 
    try: 
        data = json.loads(msg.payload.decode()) 
        producer.send(TOPIC_NAME, data) 
        producer.flush() 
        print(f"📤 MQTT → Kafka | {msg.topic} → {TOPIC_NAME}") 
    except Exception as e: 
        print(f"[ERREUR] JSON invalide : {e}") 
 
# 4. Connexion au broker MQTT 
client = mqtt.Client() 
client.connect("localhost", 1883) 
 
client.subscribe("raffinerie/temp") 
client.subscribe("raffinerie/vib") 
client.on_message = on_message 
 
print("🔁 Bridge MQTT → Kafka actif. En attente de messages...") 
client.loop_forever()