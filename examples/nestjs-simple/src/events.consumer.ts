import { Injectable, Logger } from '@nestjs/common';
import { type SnapfluxMessage, SnapfluxMessageHandler } from '@snapflux/nestjs';

@Injectable()
export class EventsConsumer {
  private readonly logger = new Logger(EventsConsumer.name);

  @SnapfluxMessageHandler('events', 'nestjs-simple')
  async handle(message: SnapfluxMessage): Promise<void> {
    this.logger.log(`Received message id=${message.id} key=${message.key}`);
    this.logger.log(`Body: ${JSON.stringify(message.body)}`);
  }
}
