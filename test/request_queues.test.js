const ow = require('ow');
const ApifyStorageLocal = require('../src/index');
const { RESOURCE_TABLE_NAME, REQUESTS_TABLE_NAME } = require('../src/request_queues');
const { uniqueKeyToRequestId } = require('../src/utils');

const TEST_QUEUES = {
    1: {
        id: 1,
        name: 'first',
        requestCount: 15,
    },
    2: {
        id: 2,
        name: 'second',
        requestCount: 35,
    },
};

/** @type ApifyStorageLocal */
let storageLocal;
let prepare;
let counter;
let markRequestHandled;
beforeEach(() => {
    storageLocal = new ApifyStorageLocal({
        inMemory: true,
        // debug: true,
    });
    prepare = sql => storageLocal.db.prepare(sql);
    counter = createCounter(storageLocal.db);
    markRequestHandled = storageLocal.db.transaction((qId, rId) => {
        prepare(`
                UPDATE ${REQUESTS_TABLE_NAME}
                SET orderNo = null
                WHERE queueId = ? AND id = ?
            `).run(qId, rId);
        prepare(`
                UPDATE ${RESOURCE_TABLE_NAME}
                SET handledRequestCount = handledRequestCount + 1
                WHERE id = ?
            `).run(qId);
    });
    seedDb(storageLocal.db);
});

afterEach(() => {
    storageLocal.closeDatabase();
});

test('queues table exists', () => {
    const { name } = prepare(`
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='${RESOURCE_TABLE_NAME}';
    `).get();
    expect(name).toBe(RESOURCE_TABLE_NAME);
});

test('requests table exists', () => {
    const { name } = prepare(`
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='${REQUESTS_TABLE_NAME}';
    `).get();
    expect(name).toBe(REQUESTS_TABLE_NAME);
});

describe('timestamps:', () => {
    const queueId = 1;
    const testInitTimestamp = Date.now();
    let selectTimestamps;
    beforeEach(() => {
        selectTimestamps = prepare(`
            SELECT modifiedAt, accessedAt, createdAt FROM ${RESOURCE_TABLE_NAME}
            WHERE id = ?
        `);
    });

    test('createdAt has a valid date', () => {
        const { createdAt } = selectTimestamps.get(queueId);
        const createdAtTimestamp = new Date(createdAt).getTime();
        expect(createdAtTimestamp).toBeGreaterThan(testInitTimestamp);
        expect(createdAtTimestamp).toBeLessThan(Date.now());
    });

    test('get updated on request UPDATE', async () => {
        const beforeUpdate = selectTimestamps.get(queueId);
        const request = numToRequest(1);
        prepare(`
            UPDATE ${REQUESTS_TABLE_NAME}
            SET retryCount = 10
            WHERE queueId = ? AND id = ?
        `).run(queueId, request.id);
        const afterUpdate = selectTimestamps.get(queueId);
        expect(new Date(afterUpdate.modifiedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.modifiedAt).getTime());
        expect(new Date(afterUpdate.accessedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.accessedAt).getTime());
    });

    test('get updated on request INSERT', async () => {
        const beforeUpdate = selectTimestamps.get(queueId);
        const request = numToRequest(100);
        request.json = 'x';
        request.queueId = queueId;
        prepare(`
            INSERT INTO ${REQUESTS_TABLE_NAME}(queueId, id, url, uniqueKey, json)
            VALUES(:queueId, :id, :url, :uniqueKey, :json)
        `).run(request);
        const afterUpdate = selectTimestamps.get(queueId);
        expect(new Date(afterUpdate.modifiedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.modifiedAt).getTime());
        expect(new Date(afterUpdate.accessedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.accessedAt).getTime());
    });

    test('get updated on request DELETE', async () => {
        const beforeUpdate = selectTimestamps.get(queueId);
        const request = numToRequest(1);
        prepare(`
            DELETE FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = ? AND id = ?
        `).run(queueId, request.id);
        const afterUpdate = selectTimestamps.get(queueId);
        expect(new Date(afterUpdate.modifiedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.modifiedAt).getTime());
        expect(new Date(afterUpdate.accessedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.accessedAt).getTime());
    });

    test('getRequest updates accessedAt', () => {
        const beforeGet = selectTimestamps.get(queueId);
        const requestId = numToRequest(1).id;
        storageLocal.requestQueues.getRequest({ queueId: `${queueId}`, requestId });
        const afterGet = selectTimestamps.get(queueId);
        expect(beforeGet.modifiedAt).toBe(afterGet.modifiedAt);
        expect(new Date(afterGet.accessedAt).getTime()).toBeGreaterThan(new Date(beforeGet.accessedAt).getTime());
    });

    test('getHead updates accessedAt', () => {
        const beforeGet = selectTimestamps.get(queueId);
        storageLocal.requestQueues.getHead({ queueId: `${queueId}` });
        const afterGet = selectTimestamps.get(queueId);
        expect(beforeGet.modifiedAt).toBe(afterGet.modifiedAt);
        expect(new Date(afterGet.accessedAt).getTime()).toBeGreaterThan(new Date(beforeGet.accessedAt).getTime());
    });
});

describe('request counts:', () => {
    const queueId = '1';
    const startCount = TEST_QUEUES[queueId].requestCount;
    let selectRequestCounts;
    beforeEach(() => {
        selectRequestCounts = prepare(`
            SELECT totalRequestCount, handledRequestCount, pendingRequestCount FROM ${RESOURCE_TABLE_NAME}
            WHERE id = ?
        `);
    });
    test('stay the same after get functions', () => {
        const requestId = numToRequest(1).id;

        storageLocal.requestQueues.getRequest({ queueId, requestId });
        let counts = selectRequestCounts.get(queueId);
        expect(counts.totalRequestCount).toBe(startCount);
        expect(counts.handledRequestCount).toBe(0);
        expect(counts.pendingRequestCount).toBe(startCount);

        storageLocal.requestQueues.getHead({ queueId });
        counts = selectRequestCounts.get(queueId);
        expect(counts.totalRequestCount).toBe(startCount);
        expect(counts.handledRequestCount).toBe(0);
        expect(counts.pendingRequestCount).toBe(startCount);
    });

    test('adding request increments totalRequestCount', async () => {
        const request = numToRequest(startCount + 5);
        request.id = undefined;

        await storageLocal.requestQueues.addRequest({ queueId, request });
        const counts = selectRequestCounts.get(queueId);
        expect(counts.totalRequestCount).toBe(startCount + 1);
        expect(counts.handledRequestCount).toBe(0);
        expect(counts.pendingRequestCount).toBe(startCount + 1);
    });

    test('adding handled request increments handledRequestCount', async () => {
        const request = numToRequest(startCount + 5);
        request.id = undefined;
        request.handledAt = new Date();

        await storageLocal.requestQueues.addRequest({ queueId, request });
        const counts = selectRequestCounts.get(queueId);
        expect(counts.totalRequestCount).toBe(startCount + 1);
        expect(counts.handledRequestCount).toBe(1);
        expect(counts.pendingRequestCount).toBe(startCount);
    });

    test('handling of unhandled request increments handledRequestCount', async () => {
        const request = numToRequest(1);
        request.handledAt = new Date();

        await storageLocal.requestQueues.updateRequest({ queueId, request });
        const counts = selectRequestCounts.get(queueId);
        expect(counts.totalRequestCount).toBe(startCount);
        expect(counts.handledRequestCount).toBe(1);
        expect(counts.pendingRequestCount).toBe(startCount - 1);
    });

    test('handling of a handled request is a no-op', async () => {
        const request = numToRequest(1);
        markRequestHandled(queueId, request.id);
        let counts = selectRequestCounts.get(queueId);
        expect(counts.handledRequestCount).toBe(1);
        request.handledAt = new Date();

        await storageLocal.requestQueues.updateRequest({ queueId, request });
        counts = selectRequestCounts.get(queueId);
        expect(counts.totalRequestCount).toBe(startCount);
        expect(counts.handledRequestCount).toBe(1);
        expect(counts.pendingRequestCount).toBe(startCount - 1);
    });

    test('un-handling of a handled request decrements handledRequestCount', async () => {
        const request = numToRequest(1);
        request.handledAt = null;
        markRequestHandled(queueId, request.id);
        let counts = selectRequestCounts.get(queueId);
        expect(counts.handledRequestCount).toBe(1);

        await storageLocal.requestQueues.updateRequest({ queueId, request });
        counts = selectRequestCounts.get(queueId);
        expect(counts.totalRequestCount).toBe(startCount);
        expect(counts.handledRequestCount).toBe(0);
        expect(counts.pendingRequestCount).toBe(startCount);
    });

    test('un-handling of a not handled request is a no-op', async () => {
        const request = numToRequest(1);
        request.handledAt = null;

        await storageLocal.requestQueues.updateRequest({ queueId, request });
        const counts = selectRequestCounts.get(queueId);
        expect(counts.totalRequestCount).toBe(startCount);
        expect(counts.handledRequestCount).toBe(0);
        expect(counts.pendingRequestCount).toBe(startCount);
    });
});

describe('getQueue', () => {
    test('returns correct queue', async () => {
        let queue = await storageLocal.requestQueues.getQueue({ queueId: '1' });
        expect(queue.id).toBe('1');
        expect(queue.name).toBe('first');
        queue = await storageLocal.requestQueues.getQueue({ queueId: '2' });
        expect(queue.id).toBe('2');
        expect(queue.name).toBe('second');
    });

    test('returns null for non-existent queues', async () => {
        const queue = await storageLocal.requestQueues.getQueue({ queueId: '3' });
        expect(queue).toBeNull();
    });
});

describe('getOrCreateQueue', () => {
    test('returns existing queue by name', async () => {
        const queue = await storageLocal.requestQueues.getOrCreateQueue({ queueName: 'first' });
        expect(queue.id).toBe('1');
        const count = counter.queues();
        expect(count).toBe(2);
    });

    test('creates a new queue with name', async () => {
        const queueName = 'third';
        const queue = await storageLocal.requestQueues.getOrCreateQueue({ queueName });
        expect(queue.id).toBe('3');
        expect(queue.name).toBe(queueName);
        const count = counter.queues();
        expect(count).toBe(3);
    });

    test('creates a new queue without name', async () => {
        const queue = await storageLocal.requestQueues.getOrCreateQueue({});
        expect(queue.id).toBe('3');
        expect(queue.name).toBeNull();
        const count = counter.queues();
        expect(count).toBe(3);
    });
});

describe('deleteQueue', () => {
    test('deletes correct queue', async () => {
        await storageLocal.requestQueues.deleteQueue({ queueId: '1' });
        const count = counter.queues();
        expect(count).toBe(1);
    });
});

describe('addRequest', () => {
    const queueId = '1';
    const startCount = TEST_QUEUES[queueId].requestCount;
    const request = numToRequest(1);
    const requestId = request.id;
    request.id = undefined;

    test('adds a request', async () => { /* eslint-disable no-shadow */
        const request = numToRequest(startCount + 1);
        const requestId = request.id;
        request.id = undefined;

        const queueOperationInfo = await storageLocal.requestQueues.addRequest({ queueId, request });
        expect(queueOperationInfo).toEqual({
            requestId,
            wasAlreadyPresent: false,
            wasAlreadyHandled: false,
        });
        expect(counter.requests(queueId)).toBe(startCount + 1);

        const requestModel = prepare(`
            SELECT * FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = ? AND id = ?
        `).get(queueId, requestId);
        expect(requestModel.queueId).toBe(Number(queueId));
        expect(requestModel.id).toBe(requestId);
        expect(requestModel.url).toBe(request.url);
        expect(requestModel.uniqueKey).toBe(request.uniqueKey);
        expect(requestModel.retryCount).toBe(0);
        expect(requestModel.method).toBe('GET');
        expect(typeof requestModel.orderNo).toBe('number');

        const savedRequest = JSON.parse(requestModel.json);
        expect(savedRequest.id).toBe(requestId);
        expect(savedRequest).toMatchObject({ ...request, id: requestId });
    });

    test('succeeds when request is already present', async () => {
        const queueOperationInfo = await storageLocal.requestQueues.addRequest({ queueId, request });
        expect(queueOperationInfo).toEqual({
            requestId,
            wasAlreadyPresent: true,
            wasAlreadyHandled: false,
        });
        expect(counter.requests(queueId)).toBe(startCount);
    });

    test('does not update request values when present', async () => {
        const newRequest = {
            ...request,
            handledAt: new Date(),
            method: 'POST',
        };

        await storageLocal.requestQueues.addRequest({ queueId, request: newRequest });
        const requestModel = prepare(`
            SELECT * FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = ? AND id = ?
        `).get(queueId, requestId);
        expect(requestModel.id).toBe(requestId);
        expect(requestModel.method).toBe('GET');
        expect(typeof requestModel.orderNo).toBe('number');

        const savedRequest = JSON.parse(requestModel.json);
        expect(savedRequest.id).toBe(requestId);
        expect(savedRequest).toMatchObject({ ...request, id: requestId });
    });

    test('succeeds when request is already handled', async () => {
        markRequestHandled(queueId, requestId);

        const queueOperationInfo = await storageLocal.requestQueues.addRequest({ queueId, request });
        expect(queueOperationInfo).toEqual({
            requestId,
            wasAlreadyPresent: true,
            wasAlreadyHandled: true,
        });
        expect(counter.requests(queueId)).toBe(startCount);
    });

    test('returns wasAlreadyHandled: false when request is added handled', async () => {
        request.handledAt = new Date();

        const queueOperationInfo = await storageLocal.requestQueues.addRequest({ queueId, request });
        expect(queueOperationInfo).toEqual({
            requestId,
            wasAlreadyPresent: true,
            wasAlreadyHandled: false,
        });
        expect(counter.requests(queueId)).toBe(startCount);
    });

    test('forefront adds request to queue head', async () => { /* eslint-disable no-shadow */
        const request = numToRequest(startCount + 1);
        const requestId = request.id;
        request.id = undefined;

        await storageLocal.requestQueues.addRequest({ queueId, request, forefront: true });
        expect(counter.requests(queueId)).toBe(startCount + 1);
        const firstId = prepare(`
            SELECT id FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = ? AND orderNo IS NOT NULL
            LIMIT 1
        `).pluck().get(queueId);
        expect(firstId).toBe(requestId);
    });

    describe('throws', () => {
        let request;
        beforeEach(() => {
            request = numToRequest(startCount + 1);
            request.id = undefined;
        });

        test('on missing url', async () => {
            delete request.url;
            try {
                await storageLocal.requestQueues.addRequest({ queueId, request });
                throw new Error('wrong-error');
            } catch (err) {
                expect(err).toBeInstanceOf(ow.ArgumentError);
            }
        });
        test('on missing uniqueKey', async () => {
            delete request.uniqueKey;
            try {
                await storageLocal.requestQueues.addRequest({ queueId, request });
                throw new Error('wrong-error');
            } catch (err) {
                expect(err).toBeInstanceOf(ow.ArgumentError);
            }
        });
        test('when id is provided', async () => {
            request.id = uniqueKeyToRequestId(request.uniqueKey);
            try {
                await storageLocal.requestQueues.addRequest({ queueId, request });
                throw new Error('wrong-error');
            } catch (err) {
                expect(err).toBeInstanceOf(ow.ArgumentError);
            }
        });
        test('when queue does not exist', async () => {
            const queueId = `${Object.keys(TEST_QUEUES).length + 1}`; // eslint-disable-line no-shadow
            try {
                await storageLocal.requestQueues.addRequest({ queueId, request });
                throw new Error('wrong-error');
            } catch (err) {
                expect(err.message).toBe(`Request queue with id: ${queueId} does not exist.`);
            }
        });
    });
});

describe('getRequest', () => {
    test('works', async () => {
        let expectedReq = numToRequest(3);
        let request = await storageLocal.requestQueues.getRequest({ queueId: '1', requestId: expectedReq.id });
        expect(request).toEqual(expectedReq);
        expectedReq = numToRequest(30);
        request = await storageLocal.requestQueues.getRequest({ queueId: '2', requestId: expectedReq.id });
        expect(request).toEqual(expectedReq);
    });
});

describe('updateRequest', () => {
    const queueId = '1';
    const startCount = TEST_QUEUES[queueId].requestCount;
    const retryCount = 2;
    const method = 'POST';
    let request;

    beforeEach(() => {
        request = {
            ...numToRequest(1),
            retryCount,
            method,
        };
    });

    test('updates a request and its info', async () => {
        const queueOperationInfo = await storageLocal.requestQueues.updateRequest({ queueId, request });
        expect(queueOperationInfo).toEqual({
            requestId: request.id,
            wasAlreadyPresent: true,
            wasAlreadyHandled: false,
        });

        const requestModel = prepare(`
            SELECT * FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = ? AND id = ?
        `).get(queueId, request.id);

        expect(requestModel.method).toBe(method);
        expect(requestModel.retryCount).toBe(retryCount);
        const requestInstance = JSON.parse(requestModel.json);
        expect(requestInstance).toEqual(request);
        expect(counter.requests(queueId)).toBe(startCount);
    });

    test('adds request when not present', async () => {
        const request = numToRequest(startCount + 1); // eslint-disable-line no-shadow

        const queueOperationInfo = await storageLocal.requestQueues.updateRequest({ queueId, request });
        expect(queueOperationInfo).toEqual({
            requestId: request.id,
            wasAlreadyPresent: false,
            wasAlreadyHandled: false,
        });
        expect(counter.requests(queueId)).toBe(startCount + 1);
    });

    test('succeeds when request is already handled', async () => {
        markRequestHandled(queueId, request.id);

        const queueOperationInfo = await storageLocal.requestQueues.updateRequest({ queueId, request });
        expect(queueOperationInfo).toEqual({
            requestId: request.id,
            wasAlreadyPresent: true,
            wasAlreadyHandled: true,
        });
        expect(counter.requests(queueId)).toBe(startCount);
    });

    test('returns wasAlreadyHandled: false when request is updated to handled', async () => {
        request.handledAt = new Date();

        const queueOperationInfo = await storageLocal.requestQueues.updateRequest({ queueId, request });
        expect(queueOperationInfo).toEqual({
            requestId: request.id,
            wasAlreadyPresent: true,
            wasAlreadyHandled: false,
        });
        expect(counter.requests(queueId)).toBe(startCount);
    });

    test('forefront moves request to queue head', async () => {
        await storageLocal.requestQueues.updateRequest({ queueId, request, forefront: true });
        expect(counter.requests(queueId)).toBe(startCount);
        const requestId = prepare(`
            SELECT id FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = ? AND orderNo IS NOT NULL
            LIMIT 1
        `).pluck().get(queueId);
        expect(requestId).toBe(request.id);
    });

    describe('throws', () => {
        test('on missing url', async () => {
            delete request.url;
            try {
                await storageLocal.requestQueues.updateRequest({ queueId, request });
                throw new Error('wrong-error');
            } catch (err) {
                expect(err).toBeInstanceOf(ow.ArgumentError);
            }
        });
        test('on missing uniqueKey', async () => {
            delete request.uniqueKey;
            try {
                await storageLocal.requestQueues.updateRequest({ queueId, request });
                throw new Error('wrong-error');
            } catch (err) {
                expect(err).toBeInstanceOf(ow.ArgumentError);
            }
        });
        test('when id is not provided', async () => {
            request.id = undefined;
            try {
                await storageLocal.requestQueues.updateRequest({ queueId, request });
                throw new Error('wrong-error');
            } catch (err) {
                expect(err).toBeInstanceOf(ow.ArgumentError);
            }
        });
        test('when queue does not exist', async () => {
            const queueId = `${Object.keys(TEST_QUEUES).length + 1}`; // eslint-disable-line no-shadow
            try {
                await storageLocal.requestQueues.updateRequest({ queueId, request });
                throw new Error('wrong-error');
            } catch (err) {
                expect(err.message).toBe(`Request queue with id: ${queueId} does not exist.`);
            }
        });
    });
});

describe('getHead', () => {
    const queueId = '2';
    const startCount = TEST_QUEUES[queueId].requestCount;

    test('fetches requests in correct order', async () => {
        const models = createRequestModels(queueId, startCount);
        const expectedItems = models
            .sort((a, b) => a.orderNo - b.orderNo)
            .map(m => JSON.parse(m.json));

        const { items } = await storageLocal.requestQueues.getHead({ queueId });
        expect(items).toEqual(expectedItems);
    });

    test('limit works', async () => {
        const limit = 10;
        const models = createRequestModels(queueId, startCount);
        const expectedItems = models
            .sort((a, b) => a.orderNo - b.orderNo)
            .slice(0, limit)
            .map(m => JSON.parse(m.json));

        const { items } = await storageLocal.requestQueues.getHead({ queueId, limit });
        expect(items).toEqual(expectedItems);
    });

    test('handled requests are not shown', async () => {
        const handledCount = 10;
        const models = createRequestModels(queueId, startCount).sort((a, b) => a.orderNo - b.orderNo);
        const modelsToHandle = models.slice(0, handledCount);
        modelsToHandle.forEach(m => markRequestHandled(queueId, m.id));
        const expectedItems = models.slice(handledCount).map(m => JSON.parse(m.json));

        const { items } = await storageLocal.requestQueues.getHead({ queueId });
        expect(items).toEqual(expectedItems);
    });
});

function seedDb(db) {
    Object.values(TEST_QUEUES).forEach((queue) => {
        const id = insertQueue(db, queue);
        expect(id).toBe(queue.id);
        const requestModels = createRequestModels(id, queue.requestCount);
        insertRequests(db, id, requestModels);
    });
}

function insertQueue(db, queue) {
    return db.prepare(`
        INSERT INTO ${RESOURCE_TABLE_NAME}(name, totalRequestCount)
        VALUES(?, ?)
    `).run(queue.name, queue.requestCount).lastInsertRowid;
}

function insertRequests(db, queueId, models) {
    const insert = db.prepare(`
        INSERT INTO ${REQUESTS_TABLE_NAME}(
            id, queueId, orderNo, url, uniqueKey, method, retryCount, json
        )
        VALUES(
            :id, :queueId, :orderNo, :url, :uniqueKey, :method, :retryCount, :json
        )
    `);
    models.forEach(model => insert.run(model));
}

function createRequestModels(queueId, count) {
    const requestModels = [];
    for (let i = 0; i < count; i++) {
        const request = numToRequest(i);
        requestModels.push({
            ...request,
            orderNo: i % 4 === 0 ? -i : i,
            queueId,
            json: JSON.stringify(request),
        });
    }
    return requestModels;
}

function numToRequest(num) {
    const url = `https://example.com/${num}`;
    return {
        id: uniqueKeyToRequestId(url),
        url,
        uniqueKey: url,
        method: 'GET',
        retryCount: 0,
        userData: {
            label: 'detail',
            foo: 'bar',
        },
    };
}

function createCounter(db) {
    const selectQueueCount = db.prepare(`SELECT COUNT(*) FROM ${RESOURCE_TABLE_NAME}`).pluck();
    const selectRequestCount = db.prepare(`SELECT COUNT(*) FROM ${REQUESTS_TABLE_NAME} WHERE queueId = ?`).pluck();
    return {
        queues() {
            return selectQueueCount.get();
        },
        requests(queueId) {
            return selectRequestCount.get(queueId);
        },
    };
}
