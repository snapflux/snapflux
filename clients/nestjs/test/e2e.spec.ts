import 'reflect-metadata';
import {Injectable} from '@nestjs/common';
import {Test, TestingModule} from '@nestjs/testing';
import type {SnapfluxMessage} from '../src';
import {SnapfluxMessageHandler, SnapfluxModule, SnapfluxService} from '../src';

const BASE_URL = process.env.SNAPFLUX_URL ?? 'http://localhost:5050';
const TOPIC = 'e2e-test-topic';
const GROUP = 'e2e-test-group';

// Defined at module level so the decorator metadata is registered once,
// before Test.createTestingModule scans for it.
const received: SnapfluxMessage[] = [];

@Injectable()
class MessageSink {
    @SnapfluxMessageHandler(TOPIC, GROUP)
    async handle(msg: SnapfluxMessage): Promise<void> {
        received.push(msg);
    }
}

/** Poll until predicate returns a non-undefined value or the timeout elapses. */
async function waitFor<T>(
    predicate: () => T | undefined | Promise<T | undefined>,
    timeoutMs = 6000,
    intervalMs = 100,
): Promise<NonNullable<T>> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const result = await predicate();
        if (result !== undefined && result !== null) return result as NonNullable<T>;
        await new Promise<void>((r) => setTimeout(r, intervalMs));
    }
    throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

describe('Snapflux NestJS Client (E2E)', () => {
    let app: TestingModule;
    let service: SnapfluxService;

    beforeAll(async () => {
        app = await Test.createTestingModule({
            imports: [
                SnapfluxModule.register({
                    baseUrl: BASE_URL,
                    consumers: [
                        {
                            topic: TOPIC,
                            group: GROUP,
                            pollingIntervalMs: 200,
                            maxMessages: 10,
                        },
                    ],
                }),
            ],
            providers: [MessageSink],
        }).compile();

        await app.init();
        service = app.get(SnapfluxService);
    });

    afterAll(async () => {
        await app.close();
    });

    beforeEach(() => {
        received.length = 0;
    });

    it('connects to the Snapflux health endpoint', async () => {
        const res = await fetch(`${BASE_URL}/health`);
        const body = (await res.json()) as { status: string };
        expect(res.status).toBe(200);
        expect(body.status).toBe('ok');
    });

    it('publishes a message and the consumer receives it', async () => {
        const payload = {hello: 'snapflux', ts: Date.now()};
        const sent = await service.send(TOPIC, {key: 'e2e-key', body: payload});

        expect(sent.id).toBeDefined();
        expect(sent.durability).toBe('durable');

        const msg = await waitFor(() => received.find((m) => m.id === sent.id));
        expect(msg.key).toBe('e2e-key');
        expect(msg.body).toMatchObject(payload);
        expect(msg.attempts).toBeGreaterThanOrEqual(1);
    });

    it('publishes multiple messages and all are consumed', async () => {
        const keys = ['alpha', 'beta', 'gamma'];
        const ids = await Promise.all(
            keys.map((key) => service.send(TOPIC, {key, body: {seq: key}}).then((r) => r.id)),
        );

        await waitFor(() => {
            const allFound = ids.every((id) => received.some((m) => m.id === id));
            return allFound ? true : undefined;
        });

        for (const id of ids) {
            expect(received.some((m) => m.id === id)).toBe(true);
        }
    });

    it('does not re-deliver a message after it is acked', async () => {
        const {id} = await service.send(TOPIC, {key: 'ack-once', body: {ack: true}});

        await waitFor(() => received.find((m) => m.id === id));

        // Wait two polling cycles — if ack failed the message would reappear.
        await new Promise<void>((r) => setTimeout(r, 500));

        const deliveries = received.filter((m) => m.id === id);
        expect(deliveries).toHaveLength(1);
    });

    it('publishes with fire-and-forget durability', async () => {
        const {id, durability} = await service.send(TOPIC, {
            key: 'ff-key',
            body: {type: 'ff'},
            durability: 'fire-and-forget',
        });

        expect(id).toBeDefined();
        expect(durability).toBe('fire-and-forget');

        await waitFor(() => received.find((m) => m.id === id));
    });

    it('publishes with strict durability', async () => {
        const {id, durability} = await service.send(TOPIC, {
            key: 'strict-key',
            body: {type: 'strict'},
            durability: 'strict',
        });

        expect(id).toBeDefined();
        expect(durability).toBe('strict');

        await waitFor(() => received.find((m) => m.id === id));
    });

    it('allows manual receive and ack without a polling consumer', async () => {
        const {id} = await service.send(TOPIC, {key: 'manual-key', body: {manual: true}});

        // Stop the automatic consumer for this topic so we can receive manually.
        // (The consumer is already running, so we just call receive directly.)
        const msg = await waitFor(async () => {
            const list = await service.receive(TOPIC, GROUP, 10);
            return list.find((m) => m.id === id);
        });

        expect(msg.id).toBe(id);
        const ackResp = await service.ack(TOPIC, GROUP, msg.receiptId);
        expect(ackResp.acknowledged).toBe(true);
    });
});
