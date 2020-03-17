const ow = require('ow');
const ApifyClientLocal = require('../src/index');
const { RESOURCE_TABLE_NAME, REQUESTS_TABLE_NAME } = require('../src/request_queues');
const { uniqueKeyToRequestId } = require('../src/utils');

describe('RequestQueues', () => {
    /** @type ApifyClientLocal */
    let client;
    let prepare;
    beforeEach(() => {
        client = new ApifyClientLocal({
            memory: true,
        });

        prepare = sql => client.db.prepare(sql);
    });

    afterEach(() => {
        client.close();
    });

    test('master table exists', () => {
        const { name } = prepare(`
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='${RESOURCE_TABLE_NAME}';
        `).get();
        expect(name).toBe(RESOURCE_TABLE_NAME);
    });

    test('Requests table exists', () => {
        const { name } = prepare(`
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='${REQUESTS_TABLE_NAME}';
        `).get();
        expect(name).toBe(REQUESTS_TABLE_NAME);
    });

    describe('methods', () => {
        let counter;

        beforeEach(() => {
            counter = createCounter(client.db);
            seedDb(client.db);
        });

        describe('getQueue', () => {
            test('works', () => {
                let queue = client.requestQueues.getQueue({ queueId: '1' });
                expect(queue.id).toBe('1');
                expect(queue.name).toBe('first');
                queue = client.requestQueues.getQueue({ queueId: '2' });
                expect(queue.id).toBe('2');
                expect(queue.name).toBe('second');
            });

            test('returns null for non-existent queues', () => {
                const queue = client.requestQueues.getQueue({ queueId: '3' });
                expect(queue).toBeNull();
            });
        });

        describe('getOrCreateQueue', () => {
            test('returns existing queue by name', () => {
                const queue = client.requestQueues.getOrCreateQueue({ queueName: 'first' });
                expect(queue.id).toBe('1');
                const count = counter.queues();
                expect(count).toBe(2);
            });

            test('creates a new queue', () => {
                const queueName = 'third';
                const queue = client.requestQueues.getOrCreateQueue({ queueName });
                expect(queue.id).toBe('3');
                expect(queue.name).toBe(queueName);
                const count = counter.queues();
                expect(count).toBe(3);
            });
        });

        describe('deleteQueue', () => {
            test('works', () => {
                client.requestQueues.deleteQueue({ queueId: '1' });
                const count = counter.queues();
                expect(count).toBe(1);
            });
        });

        describe('addRequest', () => {
            test('works with minimal values', () => {
                const queueId = '1';
                const requestCount = counter.requests(queueId);
                const request = {
                    url: 'https://example.com',
                    uniqueKey: 'https://example.com',
                };
                const queueOperationInfo = client.requestQueues.addRequest({ queueId, request });
                expect(queueOperationInfo.requestId).toBe(uniqueKeyToRequestId(request.uniqueKey));
                expect(queueOperationInfo.wasAlreadyPresent).toBe(false);
                expect(queueOperationInfo.wasAlreadyHandled).toBe(false);
                expect(counter.requests(queueId)).toBe(requestCount + 1);
            });

            describe('throws', () => {
                const queueId = '1';
                const url = 'https://example.com';
                test('on missing url', () => {
                    const request = { uniqueKey: url };
                    try {
                        client.requestQueues.addRequest({ queueId, request });
                        throw new Error('wrong-error');
                    } catch (err) {
                        expect(err).toBeInstanceOf(ow.ArgumentError);
                    }
                });
                test('on missing uniqueKey', () => {
                    const request = { url };
                    try {
                        client.requestQueues.addRequest({ queueId, request });
                        throw new Error('wrong-error');
                    } catch (err) {
                        expect(err).toBeInstanceOf(ow.ArgumentError);
                    }
                });
                test('when id is provided', () => {
                    const request = {
                        id: uniqueKeyToRequestId(url),
                        url,
                        uniqueKey: url,
                    };
                    try {
                        client.requestQueues.addRequest({ queueId, request });
                        throw new Error('wrong-error');
                    } catch (err) {
                        expect(err).toBeInstanceOf(ow.ArgumentError);
                    }
                });
            });
        });

        describe('getRequest', () => {
            test('works', () => {
                let expectedReq = numToRequest(3);
                let request = client.requestQueues.getRequest({ queueId: '1', requestId: expectedReq.id });
                expect(request).toEqual(expectedReq);
                expectedReq = numToRequest(30);
                request = client.requestQueues.getRequest({ queueId: '2', requestId: expectedReq.id });
                expect(request).toEqual(expectedReq);
            });
        });

        describe('updateRequest', () => {
            test('works', () => {
                const queueId = '1';
                const retryCount = 2;
                const method = 'POST';
                const expectedReq = numToRequest(0);
                const updatedReq = {
                    ...expectedReq,
                    retryCount,
                    method,
                };
                const queueOperationInfo = client.requestQueues.updateRequest({
                    queueId,
                    request: updatedReq,
                });
                expect(queueOperationInfo).toEqual({
                    requestId: expectedReq.id,
                    wasAlreadyPresent: true,
                    wasAlreadyHandled: false,
                });

                const requestModel = prepare(`
                    SELECT * FROM ${REQUESTS_TABLE_NAME}
                    WHERE queueId = ? AND id = ?
                `).get(queueId, expectedReq.id);

                expect(requestModel.method).toBe(method);
                expect(requestModel.retryCount).toBe(retryCount);
                const request = JSON.parse(requestModel.json);
                expect(request).toEqual({
                    ...expectedReq,
                    retryCount,
                    method,
                });
            });
        });

        describe('getHead', () => {
            test('fetches requests in correct order', () => {
                const queueId = '2';
                const { items } = client.requestQueues.getHead({ queueId });
                const models = createRequestModels(queueId, 135);
                const expectedItems = models
                    .sort((a, b) => a.orderNo - b.orderNo)
                    .slice(0, 100)
                    .map(m => JSON.parse(m.json));
                expect(items).toEqual(expectedItems);
            });
        });
    });
});

function seedDb(db) {
    const firstId = insertQueue(db, 'first');
    const secondId = insertQueue(db, 'second');
    let requestModels = createRequestModels(firstId, 15);
    insertRequests(db, firstId, requestModels);
    requestModels = createRequestModels(secondId, 135);
    insertRequests(db, firstId, requestModels);
}

function insertQueue(db, name) {
    return db.prepare(`INSERT INTO ${RESOURCE_TABLE_NAME}(name) VALUES(?)`).run(name).lastInsertRowid;
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
        method: undefined,
        retryCount: undefined,
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
