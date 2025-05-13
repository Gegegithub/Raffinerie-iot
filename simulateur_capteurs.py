import time, json, random 
import paho.mqtt.client as mqtt 
 
# Connexion au broker MQTT local 
client = mqtt.Client() 
client.connect("localhost", 1883, 60) 
 
while True: 
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ') 
 
    # Génération de données VALIDES qui passeront le filtre dans traitement_kpi.py
    # Temperature entre 30 et 150
    temp = round(random.uniform(30, 150), 2) 
    # Vibration entre 0 et 5
    vib = round(random.uniform(0, 5), 2) 
 
    msg_temp = json.dumps({ 
        "machine_id": "pipe-101", 
        "valeur": temp, 
        "timestamp": now, 
        "type_capteur": "temperature" 
    }) 
 
    msg_vib = json.dumps({ 
        "machine_id": "pump-303", 
        "valeur": vib, 
        "timestamp": now, 
        "type_capteur": "vibration" 
    }) 
 
    client.publish("raffinerie/temp", msg_temp) 
    client.publish("raffinerie/vib", msg_vib) 
 
    print(f"Publiés → Temp: {temp}°C | Vib: {vib} mm/s") 
    time.sleep(2)