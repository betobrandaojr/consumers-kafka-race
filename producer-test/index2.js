const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'appCar',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

async function run() {
  console.log('Conectando ao producer...');
  await producer.connect();
  console.log('Producer conectado!');

  
  const productsMessage = {
    "id": 4274426,
    "event_time": "2025-01-07T13:25:16.000Z",
    "insert_date": "2025-01-07T13:25:09.560Z",
    "device": {
        "imei": 869196034222469,
        "hardware": "TabletN777"
    },
    "asset": {
        "id": 590706,
        "plate": "SJY3G43",
        "prefix": "200792",
        "type_id": 17,
        "timezone": {
            "zone": "America/Sao_Paulo",
            "offset": -3
        },
        "customer_id": 40395,
        "type_description": "Caminhão Madeireiro"
    },
    "driver": {
        "id": 0,
        "name": "Não Identificado",
        "status": false,
        "language": "pt_BR",
        "badge_code": null,
        "customer_id": 1,
        "insert_date": "2023-12-21T20:00:33.222777-03:00",
        "update_date": "2023-12-21T20:00:39.40273-03:00",
        "matriculation": null
    },
    "details": {
        "fields": "4,0,792628,445037,0",
        "formId": 0,
        "formCode": 0,
        "noteCode": 991
    },
    "point": {
        "ignition": true,
        "latitude": -18.0002,
        "driver_id": -1,
        "longitude": -39.860283,
        "event_time_": "07/01/2025 10:25:16-03"
    },
    "telemetry": {
        "rpm": 0,
        "extra": 0,
        "speed": 0,
        "odometer": 792628,
        "fuelGauge": 0,
        "hourmeter": 44503.79,
        "litermeter": 0,
        "production": 0
    },
    "production": "0",
    "origin": "MQTT"
  };

  const batchSize = 1000;
  const totalMessages = 1000000;

  for (let i = 0; i < totalMessages; i += batchSize) {
    const messagesToSend = [];
    for (let j = 0; j < batchSize; j++) {
      messagesToSend.push({ value: JSON.stringify(productsMessage) });
    }

    console.log(`Enviando lote de ${batchSize} mensagens...`);
    const results = await producer.send({
      topic: 'topic.appointments',
      messages: messagesToSend,
    });

    console.log(`Lote de ${batchSize} mensagens enviado com sucesso:`, results);
  }

  console.log('Desconectando producer....');
  await producer.disconnect();
  console.log('Producer desconectado!');
}

run().catch((err) => {
  console.error('Erro ao rodar o producer:', err);
});