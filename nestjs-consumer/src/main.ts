import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { Transport, MicroserviceOptions } from '@nestjs/microservices';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        brokers: ['localhost:9092'],
        // Configurações adicionais do cliente
      },
      consumer: {
        groupId: 'nestjs-kafka',
        allowAutoTopicCreation: false, // Desative se não precisar criar tópicos automaticamente
        sessionTimeout: 15000,
        rebalanceTimeout: 60000,
        heartbeatInterval: 3000,
      },
      // Ajustes de performance do KafkaJS
      run: {
        autoCommit: true,
        autoCommitInterval: 5000, // Intervalo para commits automáticos
      },
    },
  });

  await app.startAllMicroservices();
  await app.listen(3000);

  console.log('NestJS app e microservice Kafka iniciados na porta 3000');
}
bootstrap();
