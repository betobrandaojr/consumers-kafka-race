import { Module } from '@nestjs/common';
import { KafkaController } from './kafka.controller';
import { KafkaCounterService } from './kafka-counter.service';

@Module({
  imports: [],
  controllers: [KafkaController],
  providers: [KafkaCounterService],
})
export class AppModule {}
