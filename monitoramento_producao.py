import paho.mqtt.client as mqtt
import boto3
import json
import random
import time

# Configurações MQTT e AWS IoT
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "industriamonitoramento/sensores"
AWS_REGION = "us-east-1"
AWS_IOT_ENDPOINT = "a3te1qv4nlsfqf-ats.iot.us-east-1.amazonaws.com"

# Cliente MQTT
mqtt_client = mqtt.Client()

# Função de callback quando a conexão MQTT for estabelecida
def on_connect(client, userdata, flags, rc):
    print("Conectado ao broker MQTT com código de resultado " + str(rc))
    client.subscribe(MQTT_TOPIC)

# Função de callback quando uma mensagem for recebida do broker MQTT
def on_message(client, userdata, msg):
    mensagem = msg.payload.decode()
    print(f"Mensagem recebida do tópico {msg.topic}: {mensagem}")
    enviar_para_aws(json.loads(mensagem))

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Conectar ao broker MQTT
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Cliente AWS IoT
aws_client = boto3.client('iot-data', region_name=AWS_REGION)

# Função para enviar dados para AWS IoT
def enviar_para_aws(dados):
    response = aws_client.publish(
        topic=MQTT_TOPIC,
        qos=1,
        payload=json.dumps(dados)
    )
    print("Dados enviados para AWS IoT: " + str(response))

# Função para simular dados do sensor
def simular_dados_sensor():
    return {
        "temperatura": random.uniform(20.0, 100.0),
        "vibracao": random.uniform(0.0, 10.0),
        "umidade": random.uniform(30.0, 90.0),
        "timestamp": int(time.time())
    }

# Loop principal para enviar dados simulados para o broker MQTT
try:
    mqtt_client.loop_start()
    while True:
        dados_sensor = simular_dados_sensor()
        mqtt_client.publish(MQTT_TOPIC, json.dumps(dados_sensor))
        print(f"Dados do sensor enviados: {dados_sensor}")
        time.sleep(5)
except KeyboardInterrupt:
    mqtt_client.loop_stop()
    print("Encerrando o monitoramento.")
