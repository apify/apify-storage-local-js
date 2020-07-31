const fs = require('fs-extra');
const path = require('path');
const ApifyStorageLocal = require('../src/index');
const { STORAGE_NAMES } = require('../src/consts');
const { prepareTestDir, removeTestDir } = require('./_tools');

const TEST_STORES = {
    1: {
        id: 1,
        name: 'first',
        recordCount: 5,
    },
    2: {
        id: 2,
        recordCount: 35,
    },
};

/** @type ApifyStorageLocal */
let storageLocal;
let prepare;
let counter;
let STORAGE_DIR;
beforeEach(() => {
    STORAGE_DIR = prepareTestDir();
    storageLocal = new ApifyStorageLocal({
        storageDir: STORAGE_DIR,
    });
    counter = createCounter(STORAGE_DIR);
    seed(STORAGE_DIR);
});

afterAll(() => {
    removeTestDir(STORAGE_NAMES.KEY_VALUE_STORES);
});

test('stores directory exists', () => {
    const subDirs = fs.readdirSync(STORAGE_DIR);
    expect(subDirs).toContain(STORAGE_NAMES.KEY_VALUE_STORES);
});

describe('timestamps:', () => {
    const storeId = 1;
    const testInitTimestamp = Date.now();
    function getTimestamps(id) {
        const stats = fs.statSync(idToDir(STORAGE_DIR, id));
        return {
            accessedAt: stats.atime,
            modifiedAt: stats.mtime,
            createdAt: stats.birthtime,
        };
    }

    test('createdAt has a valid date', () => {
        const { createdAt } = getTimestamps(storeId);
        const createdAtTimestamp = new Date(createdAt).getTime();
        expect(createdAtTimestamp).toBeGreaterThan(testInitTimestamp);
        expect(createdAtTimestamp).toBeLessThan(Date.now());
    });

    test('get updated on record update', () => {
        const beforeUpdate = getTimestamps(storeId);
        const record = numToRecord(1);
        fs.writeFileSync(path.join(idToDir(STORAGE_DIR, storeId), record.filename), 'abc');
        const afterUpdate = getTimestamps(storeId);
        expect(new Date(afterUpdate.modifiedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.modifiedAt).getTime());
        expect(new Date(afterUpdate.accessedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.accessedAt).getTime());
    });

    test('get updated on record insert', () => {
        const beforeUpdate = getTimestamps(storeId);
        const record = numToRecord(100);
        fs.writeFileSync(path.join(idToDir(STORAGE_DIR, storeId), record.filename), record.data);
        const afterUpdate = getTimestamps(storeId);
        expect(new Date(afterUpdate.modifiedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.modifiedAt).getTime());
        expect(new Date(afterUpdate.accessedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.accessedAt).getTime());
    });

    test('get updated on record delete', () => {
        const beforeUpdate = getTimestamps(storeId);
        const record = numToRecord(1);
        fs.unlinkSync(path.join(idToDir(STORAGE_DIR, storeId), record.filename));
        const afterUpdate = getTimestamps(storeId);
        expect(new Date(afterUpdate.modifiedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.modifiedAt).getTime());
        expect(new Date(afterUpdate.accessedAt).getTime()).toBeGreaterThan(new Date(beforeUpdate.accessedAt).getTime());
    });

    test('getValue updates accessedAt', async () => {
        const beforeGet = getTimestamps(storeId);
        const { key } = numToRecord(1);
        await storageLocal.keyValueStore(`${storeId}`).getValue(key);
        const afterGet = getTimestamps(storeId);
        expect(beforeGet.modifiedAt).toBe(afterGet.modifiedAt);
        expect(new Date(afterGet.accessedAt).getTime()).toBeGreaterThan(new Date(beforeGet.accessedAt).getTime());
    });

    test('listKeys updates accessedAt', async () => {
        const beforeGet = getTimestamps(storeId);
        await storageLocal.keyValueStore(`${storeId}`).listKeys();
        const afterGet = getTimestamps(storeId);
        expect(beforeGet.modifiedAt).toBe(afterGet.modifiedAt);
        expect(new Date(afterGet.accessedAt).getTime()).toBeGreaterThan(new Date(beforeGet.accessedAt).getTime());
    });
});

describe('get store', () => {
    test('returns correct store', async () => {
        let queue = await storageLocal.keyValueStore('1').get();
        expect(queue.id).toBe('1');
        expect(queue.name).toBe('first');
        queue = await storageLocal.keyValueStore('2').get();
        expect(queue.id).toBe('2');
        expect(queue.name).toBe('second');
    });

    test('returns undefined for non-existent queues', async () => {
        const queue = await storageLocal.keyValueStore('3').get();
        expect(queue).toBeUndefined();
    });
});

describe('getOrCreate', () => {
    test('returns existing queue by name', async () => {
        const queue = await storageLocal.keyValueStores().getOrCreate('first');
        expect(queue.id).toBe('1');
        const count = counter.stores();
        expect(count).toBe(2);
    });

    test('creates a new queue with name', async () => {
        const queueName = 'third';
        const queue = await storageLocal.keyValueStores().getOrCreate(queueName);
        expect(queue.id).toBe('3');
        expect(queue.name).toBe(queueName);
        const count = counter.stores();
        expect(count).toBe(3);
    });

    test('creates a new queue without name', async () => {
        let queue = await storageLocal.keyValueStores().getOrCreate();
        expect(queue.id).toBe('3');
        expect(queue.name).toBeUndefined();
        let count = counter.stores();
        expect(count).toBe(3);
        queue = await storageLocal.keyValueStores().getOrCreate();
        expect(queue.id).toBe('4');
        expect(queue.name).toBeUndefined();
        count = counter.stores();
        expect(count).toBe(4);
    });
});

describe('delete store', () => {
    test('deletes correct store', async () => {
        await storageLocal.keyValueStore('1').delete();
        const count = counter.stores();
        expect(count).toBe(1);
    });
});

describe('setValue', () => {
    const queueId = '1';
    const startCount = TEST_STORES[queueId].recordCount;
    const value = numToRecord(1);
    const recordId = value.id;
    value.id = undefined;

    test('adds a value', async () => { /* eslint-disable no-shadow */
        const record = numToRecord(startCount + 1);
        const recordId = record.id;
        record.id = undefined;

        const queueOperationInfo = await storageLocal.keyValueStore(queueId).setValue(record);
        expect(queueOperationInfo).toEqual({
            recordId,
            wasAlreadyPresent: false,
            wasAlreadyHandled: false,
        });
        expect(counter.records(queueId)).toBe(startCount + 1);

        const recordModel = prepare(`
            SELECT * FROM ${STORAGE_NAMES.record_QUEUE_recordS}
            WHERE queueId = ? AND id = ?
        `).get(queueId, recordId);
        expect(recordModel.queueId).toBe(Number(queueId));
        expect(recordModel.id).toBe(recordId);
        expect(recordModel.url).toBe(record.url);
        expect(recordModel.uniqueKey).toBe(record.uniqueKey);
        expect(recordModel.retryCount).toBe(0);
        expect(recordModel.method).toBe('GET');
        expect(typeof recordModel.orderNo).toBe('number');

        const savedrecord = JSON.parse(recordModel.json);
        expect(savedrecord.id).toBe(recordId);
        expect(savedrecord).toMatchObject({ ...record, id: recordId });
    });

    test('updates when key is already present', async () => {
        const queueOperationInfo = await storageLocal.keyValueStore(queueId).setValue(value);
        expect(queueOperationInfo).toEqual({
            recordId,
            wasAlreadyPresent: true,
            wasAlreadyHandled: false,
        });
        expect(counter.records(queueId)).toBe(startCount);
    });

    describe('throws', () => {
        test('when store does not exist', async () => {
            const storeId = `${Object.keys(TEST_STORES).length + 1}`; // eslint-disable-line no-shadow
            try {
                await storageLocal.keyValueStore(storeId).setValue('some-key', 'some-value');
                throw new Error('wrong-error');
            } catch (err) {
                expect(err.message).toBe(`Key-value store with id: ${storeId} does not exist.`);
            }
        });
    });
});

describe('getValue', () => {
    const storeId = '1';
    const startCount = TEST_STORES[storeId].recordCount;

    test('gets values', async () => {
        let expectedRecord = numToRecord(3);
        let record = await storageLocal.keyValueStore(storeId).getValue(expectedRecord.key);
        expect(record).toEqual(expectedRecord);
        expectedRecord = numToRecord(30);
        record = await storageLocal.keyValueStore('2').getValue(expectedRecord.key);
        expect(record).toEqual(expectedRecord);
    });

    test('returns undefined for non-existent records', async () => {
        const expectedRecord = numToRecord(startCount + 1);
        const record = await storageLocal.keyValueStore('1').getValue(expectedRecord.key);
        expect(record).toBeUndefined();
    });
});

describe('listKeys', () => {
    const queueId = '2';
    const startCount = TEST_STORES[queueId].recordCount;

    test('fetches keys in correct order', async () => {
        const models = createRecords(queueId, startCount);
        const expectedItems = models
            .sort((a, b) => a.orderNo - b.orderNo)
            .map((m) => JSON.parse(m.json));

        const { items } = await storageLocal.keyValueStore(queueId).listKeys();
        expect(items).toEqual(expectedItems);
    });

    test('limit works', async () => {
        const limit = 10;
        const models = createRecords(queueId, startCount);
        const expectedItems = models
            .sort((a, b) => a.orderNo - b.orderNo)
            .slice(0, limit)
            .map((m) => JSON.parse(m.json));

        const { items } = await storageLocal.keyValueStore(queueId).listKeys({ limit });
        expect(items).toEqual(expectedItems);
    });
});

function seed(dir) {
    Object.values(TEST_STORES).forEach((store) => {
        const storeDir = insertStore(dir, store);
        const records = createRecords(store);
        insertRecords(storeDir, records);
    });
}

function insertStore(dir, store) {
    const storeDirName = store.name || idToDirname(store.id);
    const storeDir = path.join(dir, storeDirName);
    fs.ensureDirSync(storeDir);
    fs.emptyDirSync(storeDir);
    return storeDir;
}

function insertRecords(dir, records) {
    records.forEach((record) => {
        const filePath = path.join(dir, record.filename);
        fs.writeFileSync(filePath, record.data);
    });
}

function createRecords(store) {
    const records = [];
    for (let i = 0; i < store.recordCount; i++) {
        const record = numToRecord(i);
        records.push(record);
    }
    return records;
}

function numToRecord(num) {
    if (num % 3 === 0) {
        const key = `markup_${num}`;
        return {
            key,
            data: `<html><body>${num}: ✅</body></html>`,
            filename: `${key}.html`,
            contentType: 'text/html',
        };
    }
    if (num % 7 === 0) {
        const key = `buffer_${num}`;
        const chunks = Array(5000).fill(`${num}🚀🎯`);
        return {
            key,
            data: Buffer.from(chunks),
            filename: `${key}.bin`,
            contentType: 'application/octet-stream',
        };
    }
    const key = `object_${num}`;
    const filename = `${key}.json`;
    return {
        key,
        data: JSON.stringify({ number: num, filename }),
        filename,
        contentType: 'application/json',
    };
}

function createCounter(dir) {
    return {
        stores() {
            return fs.readdirSync(path.join(dir, STORAGE_NAMES.KEY_VALUE_STORES)).length;
        },
        records(storeId) {
            return fs.readdirSync(idToDir(dir, storeId)).length;
        },
    };
}

function idToDir(storageDir, id) {
    return path.join(storageDir, STORAGE_NAMES.KEY_VALUE_STORES, idToDirname(id));
}

function idToDirname(id) {
    return `id_${id}`;
}
