const { promisify } = require('util');
const ApifyClientLocal = require('../src/index');
const { RESOURCE_TABLE_NAME, REQUESTS_TABLE_NAME } = require('../src/request_queues');
const { promisifyDbRun } = require('../src/utils');

describe('RequestQueues', () => {
    let client;
    let run;
    let get;
    beforeEach(async () => {
        client = new ApifyClientLocal({
            inMemory: true,
            debug: true,
        });
        run = promisifyDbRun(client.db);
        get = promisify(client.db.get.bind(client.db));

        // This is internal, but useful for tests.
        await client.requestQueues.initPromise;
    });

    afterEach(async () => {
        await client.destroy();
    });

    test('master table exists', async () => {
        const { name } = await get(`
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='${RESOURCE_TABLE_NAME}';
        `);
        expect(name).toBe(RESOURCE_TABLE_NAME);
    });

    test('Requests table exists', async () => {
        const { name } = await get(`
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='${REQUESTS_TABLE_NAME}';
        `);
        expect(name).toBe(REQUESTS_TABLE_NAME);
    });

    describe('methods', () => {
        const dummyQ = 'dummy';
        let result;
        beforeEach(async () => {
            result = await run(`
                INSERT INTO ${RESOURCE_TABLE_NAME}(name)
                VALUES('${dummyQ}')
            `);
        });
        test('.getQueue works', async () => {
            const queue = await client.requestQueues.getQueue({ queueId: result.lastID });
            expect(queue.name).toBe('dummy');
        });

        test('.getQueue returns null for non-existent queues', async () => {
            const queue = await client.requestQueues.getQueue({ queueId: result.lastID + 1 });
            expect(queue).toBeNull();
        });

        test('.getOrCreateQueue returns existing queue by name', async () => {
            const queue = await client.requestQueues.getOrCreateQueue({ queueName: dummyQ });
            expect(queue.id).toBe(result.lastID);
            const { queueCount } = await get(`SELECT COUNT(*) as queueCount FROM ${RESOURCE_TABLE_NAME}`);
            expect(queueCount).toBe(1);
        });

        test('.getOrCreateQueue returns existing queue by ID', async () => {
            const queue = await client.requestQueues.getOrCreateQueue({ queueName: result.lastID });
            expect(queue.id).toBe(result.lastID);
            const { queueCount } = await get(`SELECT COUNT(*) as queueCount FROM ${RESOURCE_TABLE_NAME}`);
            expect(queueCount).toBe(1);
        });

        test('.getOrCreateQueue creates a new queue', async () => {
            const queueName = `x${dummyQ}x`;
            const queue = await client.requestQueues.getOrCreateQueue({ queueName });
            expect(queue.id).toBe(result.lastID + 1);
            expect(queue.name).toBe(queueName);
            const { queueCount } = await get(`SELECT COUNT(*) as queueCount FROM ${RESOURCE_TABLE_NAME}`);
            expect(queueCount).toBe(2);
        });
    });
});
