import { DiscoveryService } from '@golevelup/nestjs-discovery';
import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { SNAPFLUX_MESSAGE_HANDLER, SNAPFLUX_OPTIONS } from './snapflux.constants';
import type {
  AckResponse,
  ConsumerConfig,
  MessageHandlerMeta,
  SendRequest,
  SendResponse,
  SnapfluxMessage,
  SnapfluxOptions,
} from './snapflux.types';

@Injectable()
export class SnapfluxService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SnapfluxService.name);
  private readonly abortControllers = new Map<string, AbortController>();

  constructor(
    @Inject(SNAPFLUX_OPTIONS) private readonly options: SnapfluxOptions,
    private readonly discover: DiscoveryService,
  ) {}

  async onModuleInit(): Promise<void> {
    const handlers =
      await this.discover.providerMethodsWithMetaAtKey<MessageHandlerMeta>(SNAPFLUX_MESSAGE_HANDLER);

    for (const handler of handlers) {
      const { topic, group } = handler.meta;
      const consumerConfig = this.options.consumers?.find(
        (c) => c.topic === topic && c.group === group,
      );

      if (!consumerConfig) {
        this.logger.warn(
          `No consumer config for topic="${topic}" group="${group}" — register it in SnapfluxModule.register({ consumers: [...] })`,
        );
        continue;
      }

      const key = `${topic}:${group}`;
      if (this.abortControllers.has(key)) {
        this.logger.warn(`Duplicate handler for topic="${topic}" group="${group}", skipping`);
        continue;
      }

      const ac = new AbortController();
      this.abortControllers.set(key, ac);

      const boundHandler = handler.discoveredMethod.handler.bind(
        handler.discoveredMethod.parentClass.instance,
      ) as (msg: SnapfluxMessage) => Promise<void> | void;

      this.startPolling(consumerConfig, boundHandler, ac.signal);
      this.logger.log(`Consumer started: topic="${topic}" group="${group}"`);
    }
  }

  onModuleDestroy(): void {
    for (const ac of this.abortControllers.values()) {
      ac.abort();
    }
    this.abortControllers.clear();
  }

  /** Publish a message to a topic. */
  async send(topic: string, req: SendRequest): Promise<SendResponse> {
    const url = `${this.options.baseUrl}/v1/topics/${encodeURIComponent(topic)}/messages`;
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req),
    });
    if (!res.ok) {
      throw new Error(`Snapflux send failed [${res.status}]: ${await res.text()}`);
    }
    return res.json() as Promise<SendResponse>;
  }

  /** Pull up to `limit` messages from a topic/group. */
  async receive(
    topic: string,
    group: string,
    limit = 10,
    signal?: AbortSignal,
  ): Promise<SnapfluxMessage[]> {
    const url =
      `${this.options.baseUrl}/v1/topics/${encodeURIComponent(topic)}/messages` +
      `?group=${encodeURIComponent(group)}&limit=${limit}`;
    const res = await fetch(url, { signal });
    if (!res.ok) {
      throw new Error(`Snapflux receive failed [${res.status}]: ${await res.text()}`);
    }
    return res.json() as Promise<SnapfluxMessage[]>;
  }

  /** Acknowledge a delivered message. */
  async ack(topic: string, group: string, receiptId: string): Promise<AckResponse> {
    const url =
      `${this.options.baseUrl}/v1/topics/${encodeURIComponent(topic)}/messages/${encodeURIComponent(receiptId)}` +
      `?group=${encodeURIComponent(group)}`;
    const res = await fetch(url, { method: 'DELETE' });
    if (!res.ok) {
      throw new Error(`Snapflux ack failed [${res.status}]: ${await res.text()}`);
    }
    return res.json() as Promise<AckResponse>;
  }

  private startPolling(
    config: ConsumerConfig,
    handler: (msg: SnapfluxMessage) => Promise<void> | void,
    signal: AbortSignal,
  ): void {
    const { topic, group, pollingIntervalMs = 1000, maxMessages = 10 } = config;

    const poll = async (): Promise<void> => {
      if (signal.aborted) return;

      try {
        const messages = await this.receive(topic, group, maxMessages, signal);
        for (const msg of messages) {
          if (signal.aborted) return;
          try {
            await handler(msg);
            await this.ack(topic, group, msg.receiptId);
          } catch (err) {
            this.logger.error(
              `Handler threw for topic="${topic}" group="${group}" msgId="${msg.id}"`,
              err,
            );
            // Message remains in-flight; requeue will reclaim it after visibility timeout.
          }
        }
      } catch (err) {
        if (!signal.aborted) {
          this.logger.error(`Poll error for topic="${topic}" group="${group}"`, err);
        }
      }

      if (!signal.aborted) {
        setTimeout(() => void poll(), pollingIntervalMs);
      }
    };

    void poll();
  }
}
