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
    id: "1",
    model: 'Elantra',
    color: 'blue',
  };

  console.log('Enviando mensagem...');
  const results = await producer.send({
    topic: 'topic.cars', 
    messages: [{ value: JSON.stringify(productsMessage) }],
  });

  console.log('Mensagens enviadas com sucesso:', results);

  console.log('Desconectando producer...');
  await producer.disconnect();
  console.log('Producer desconectado!');
}

run().catch((err) => {
  console.error('Erro ao rodar o producer:', err);
});

//node index.js