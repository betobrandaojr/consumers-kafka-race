import { Injectable, Logger } from '@nestjs/common';

@Injectable()
export class KafkaCounterService {
  private readonly logger = new Logger(KafkaCounterService.name);

  private readonly targetCount = 1_000_000;
  private messageCount = 0;
  private startTime: number | null = null;

  processMessage() {
    if (this.messageCount === 0) {
      this.startTime = Date.now();
      this.logger.log('Recebemos a primeira mensagem. Contagem iniciada...');
    }

    this.messageCount++;

    if (this.messageCount === this.targetCount) {
      const endTime = Date.now();
      const totalTimeMs = endTime - (this.startTime || endTime);
      this.logger.log(
        `Processamos ${this.targetCount} mensagens em ${totalTimeMs} ms (${(
          totalTimeMs / 1000
        ).toFixed(2)} s).`,
      );
      this.resetCounter();
    }
  }

  private resetCounter() {
    this.messageCount = 0;
    this.startTime = null;
    this.logger.log('Contagem resetada. Pronto para iniciar nova contagem.');
  }
}
