import { Controller } from '@nestjs/common';
import { MessagePattern, Payload } from '@nestjs/microservices';
import { KafkaCounterService } from './kafka-counter.service';

@Controller()
export class KafkaController {
  constructor(private readonly kafkaCounterService: KafkaCounterService) {}

  @MessagePattern('topic.appointments')
  async consumeAppointments(@Payload() message: any) {
    this.kafkaCounterService.processMessage();
  }
}
