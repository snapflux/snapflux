import type { ModuleMetadata, Type } from '@nestjs/common';

export type Durability = 'fire-and-forget' | 'durable' | 'strict';

export interface SendRequest {
  key: string;
  body: unknown;
  durability?: Durability;
}

export interface SendResponse {
  id: string;
  durability?: Durability;
}

export interface SnapfluxMessage {
  id: string;
  receiptId: string;
  key: string;
  body: unknown;
  attempts: number;
  sentAt: string;
}

export interface AckResponse {
  receiptId: string;
  acknowledged: boolean;
}

export interface ConsumerConfig {
  /** Topic to poll. */
  topic: string;
  /** Consumer group name. */
  group: string;
  /** How often to poll for new messages (ms). Default: 1000. */
  pollingIntervalMs?: number;
  /** Max messages per poll call. Default: 10, max 100. */
  maxMessages?: number;
}

export interface SnapfluxOptions {
  /** Base URL of the Snapflux HTTP API, e.g. http://localhost:5050 */
  baseUrl: string;
  /** Consumer configurations — one per topic/group pair. */
  consumers?: ConsumerConfig[];
}

export interface SnapfluxOptionsFactory {
  createOptions(): Promise<SnapfluxOptions> | SnapfluxOptions;
}

export interface SnapfluxModuleAsyncOptions extends Pick<ModuleMetadata, 'imports'> {
  useExisting?: Type<SnapfluxOptionsFactory>;
  useClass?: Type<SnapfluxOptionsFactory>;
  useFactory?: (...args: unknown[]) => Promise<SnapfluxOptions> | SnapfluxOptions;
  inject?: unknown[];
}

export interface MessageHandlerMeta {
  topic: string;
  group: string;
}
