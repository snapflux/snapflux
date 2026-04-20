import { SetMetadata } from '@nestjs/common';
import { SNAPFLUX_MESSAGE_HANDLER } from './snapflux.constants';
import type { MessageHandlerMeta } from './snapflux.types';

/**
 * Marks a method as a Snapflux message handler for the given topic and group.
 *
 * The method receives a single `SnapfluxMessage` argument. After it resolves
 * without throwing, the message is automatically acknowledged.
 *
 * @example
 * ```ts
 * @SnapfluxMessageHandler('orders', 'billing-service')
 * async handleOrder(message: SnapfluxMessage) {
 *   console.log(message.body);
 * }
 * ```
 */
export const SnapfluxMessageHandler = (topic: string, group: string) =>
  SetMetadata<string, MessageHandlerMeta>(SNAPFLUX_MESSAGE_HANDLER, { topic, group });
