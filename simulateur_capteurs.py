import time, json, random 
import paho.mqtt.client as mqtt 
import math

# Connexion au broker MQTT local 
client = mqtt.Client() 
client.connect("localhost", 1883, 60)

# Paramètres pour le cycle de température
temp_min = 90  # Température minimale en °C
temp_max = 170  # Température maximale en °C

# Variable pour suivre la direction de la température (montée ou descente)
temp_current = temp_min
temp_increasing = True
temp_step = 2  # Changement de température à chaque itération

while True: 
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Calcul de la température avec augmentation/diminution progressive
    if temp_increasing:
        temp_current += temp_step
        if temp_current >= temp_max:
            temp_current = temp_max
            temp_increasing = False
    else:
        temp_current -= temp_step
        if temp_current <= temp_min:
            temp_current = temp_min
            temp_increasing = True
    
    temp = round(temp_current, 1)
    
    # Vibration aléatoire entre 0 et 5
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
 
    direction = "↗" if temp_increasing else "↘"
    print(f"Publiés → Temp: {temp}°C {direction} | Vib: {vib} mm/s") 
    
    time.sleep(2)