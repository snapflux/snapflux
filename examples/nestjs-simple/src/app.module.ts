import { Module } from '@nestjs/common';
import { SnapfluxModule } from '@snapflux/nestjs';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsConsumer } from './events.consumer';

@Module({
  imports: [
    SnapfluxModule.register({
      baseUrl: process.env.SNAPFLUX_URL ?? 'http://localhost:5050',
      consumers: [{ topic: 'events', group: 'nestjs-simple', pollingIntervalMs: 1000 }],
    }),
  ],
  controllers: [AppController],
  providers: [AppService, EventsConsumer],
})
export class AppModule {}
