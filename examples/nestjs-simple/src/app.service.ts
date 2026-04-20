import { Injectable } from '@nestjs/common';
import { SnapfluxService } from '@snapflux/nestjs';

@Injectable()
export class AppService {
  constructor(private readonly snapflux: SnapfluxService) {}

  getHello(): string {
    return 'Hello World!';
  }

  async publishEvent(payload: Record<string, unknown>) {
    const response = await this.snapflux.send('events', { key: 'event', body: payload, durability: 'durable' });
    return { published: true, id: response.id };
  }
}
