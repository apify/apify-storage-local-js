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
        let result;
        beforeEach(async () => {
            result = await run(`
                INSERT INTO ${RESOURCE_TABLE_NAME}(name)
                VALUES('dummy')
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
    });
});
