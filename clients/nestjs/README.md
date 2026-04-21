# @snapflux/nestjs

NestJS client for the [Snapflux](../../) event store.

## Installation

```bash
npm install @snapflux/nestjs
```

Peer dependencies (`@nestjs/common` and `@nestjs/core`) v6–v11 are supported.

## Quick start

### 1. Register the module

```ts
import { SnapfluxModule } from '@snapflux/nestjs';

@Module({
  imports: [
    SnapfluxModule.register({
      baseUrl: 'http://localhost:5050',
      consumers: [
        { topic: 'orders', group: 'billing-service', pollingIntervalMs: 500 },
      ],
    }),
  ],
})
export class AppModule {}
```

### 2. Publish a message

Inject `SnapfluxService` anywhere and call `send`:

```ts
import { SnapfluxService } from '@snapflux/nestjs';

@Injectable()
export class OrderService {
  constructor(private readonly snapflux: SnapfluxService) {}

  async placeOrder(order: Order) {
    await this.snapflux.send('orders', { key: order.id, body: order });
  }
}
```

### 3. Consume messages

Decorate a provider method with `@SnapfluxMessageHandler`. The message is automatically acknowledged after the method returns without throwing.

```ts
import { SnapfluxMessage, SnapfluxMessageHandler } from '@snapflux/nestjs';

@Injectable()
export class BillingService {
  @SnapfluxMessageHandler('orders', 'billing-service')
  async handle(msg: SnapfluxMessage) {
    console.log(msg.body);
  }
}
```

The `topic` and `group` must match an entry in the `consumers` array passed to `SnapfluxModule.register`.

## API reference

### `SnapfluxModule`

| Method | Description |
|---|---|
| `register(options)` | Synchronous configuration |
| `registerAsync(options)` | Async configuration — supports `useFactory`, `useClass`, `useExisting` |

### `SnapfluxService`

| Method | Description |
|---|---|
| `send(topic, req)` | Publish a message; returns `{ id, durability }` |
| `receive(topic, group, limit?, signal?)` | Pull up to `limit` messages (default 10) |
| `ack(topic, group, receiptId)` | Acknowledge a delivered message |

### `SendRequest`

| Field | Type | Description |
|---|---|---|
| `key` | `string` | Partition key |
| `body` | `unknown` | Message payload |
| `durability` | `'fire-and-forget' \| 'durable' \| 'strict'` | Optional; default is `durable` |

### `SnapfluxMessage`

| Field | Type | Description |
|---|---|---|
| `id` | `string` | Stable message ID |
| `receiptId` | `string` | Delivery-specific ID used for ack |
| `key` | `string` | Partition key the message was sent with |
| `body` | `unknown` | Message payload |
| `attempts` | `number` | How many times delivery has been attempted |
| `sentAt` | `string` | ISO timestamp |

### `ConsumerConfig`

| Field | Type | Default | Description |
|---|---|---|---|
| `topic` | `string` | — | Topic to poll |
| `group` | `string` | — | Consumer group name |
| `pollingIntervalMs` | `number` | `1000` | Milliseconds between poll calls |
| `maxMessages` | `number` | `10` | Max messages per poll (hard max 100) |

## Async configuration

```ts
SnapfluxModule.registerAsync({
  imports: [ConfigModule],
  useFactory: (cfg: ConfigService) => ({
    baseUrl: cfg.get('SNAPFLUX_URL'),
    consumers: [{ topic: 'orders', group: 'billing-service' }],
  }),
  inject: [ConfigService],
});
```

## Running tests

The E2E test suite requires a running Snapflux instance:

```bash
# point at a local instance (default: http://localhost:5050)
SNAPFLUX_URL=http://localhost:5050 npm test
```
