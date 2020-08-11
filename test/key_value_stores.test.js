const fs = require('fs-extra');
const path = require('path');
const ApifyStorageLocal = require('../src/index');
const { STORAGE_NAMES } = require('../src/consts');
const { prepareTestDir, removeTestDir } = require('./_tools');

const TEST_STORES = {
    1: {
        name: 'first',
        recordCount: 5,
    },
    2: {
        name: 'second',
        recordCount: 35,
    },
};

/** @type ApifyStorageLocal */
let storageLocal;
let counter;
let STORAGE_DIR;
let storeNameToDir;
beforeEach(() => {
    STORAGE_DIR = prepareTestDir();
    storageLocal = new ApifyStorageLocal({
        storageDir: STORAGE_DIR,
    });
    const keyValueStoresDir = path.join(STORAGE_DIR, STORAGE_NAMES.KEY_VALUE_STORES);
    storeNameToDir = (storeName) => {
        return path.join(keyValueStoresDir, storeName);
    };
    counter = createCounter(keyValueStoresDir);
    seed(keyValueStoresDir);
});

afterAll(() => {
    removeTestDir(STORAGE_NAMES.KEY_VALUE_STORES);
});

test('stores directory exists', () => {
    const subDirs = fs.readdirSync(STORAGE_DIR);
    expect(subDirs).toContain(STORAGE_NAMES.KEY_VALUE_STORES);
});

describe('timestamps:', () => {
    const storeName = 'first';
    const testInitTimestamp = Date.now();
    function getStats(name) {
        const stats = fs.statSync(storeNameToDir(name));
        return {
            accessedAt: stats.atime,
            modifiedAt: stats.mtime,
            createdAt: stats.birthtime,
        };
    }

    test('createdAt has a valid date', async () => {
        const { createdAt } = await storageLocal.keyValueStore(storeName).get();
        const createdAtTimestamp = createdAt.getTime();
        expect(createdAtTimestamp).toBeGreaterThan(testInitTimestamp);
        expect(createdAtTimestamp).toBeLessThan(Date.now());
    });

    // Record updates do not update timestamps on folders and thus
    // this will never work. It's probably not that important for local.
    test.skip('get updated on record update', () => {
        const beforeUpdate = getStats(storeName);
        const record = numToRecord(1);
        fs.writeFileSync(path.join(storeNameToDir(storeName), record.filename), 'abc');
        const afterUpdate = getStats(storeName);
        expect(afterUpdate.modifiedAt.getTime()).toBeGreaterThan(beforeUpdate.modifiedAt.getTime());
        expect(afterUpdate.accessedAt.getTime()).toBeGreaterThan(beforeUpdate.accessedAt.getTime());
    });

    test('get updated on record insert', async () => {
        const beforeUpdate = getStats(storeName);
        const record = numToRecord(100);
        fs.writeFileSync(path.join(storeNameToDir(storeName), record.filename), record.data);
        const afterUpdate = await storageLocal.keyValueStore(storeName).get();
        expect(afterUpdate.modifiedAt.getTime()).toBeGreaterThan(beforeUpdate.modifiedAt.getTime());
        expect(afterUpdate.accessedAt.getTime()).toBeGreaterThan(beforeUpdate.accessedAt.getTime());
    });

    test('get updated on record delete', async () => {
        const beforeUpdate = getStats(storeName);
        const record = numToRecord(1);
        fs.unlinkSync(path.join(storeNameToDir(storeName), record.filename));
        const afterUpdate = await storageLocal.keyValueStore(storeName).get();
        expect(afterUpdate.modifiedAt.getTime()).toBeGreaterThan(beforeUpdate.modifiedAt.getTime());
        expect(afterUpdate.accessedAt.getTime()).toBeGreaterThan(beforeUpdate.accessedAt.getTime());
    });

    test('getValue updates accessedAt', async () => {
        const beforeGet = getStats(storeName);
        const { key } = numToRecord(1);
        await storageLocal.keyValueStore(storeName).getValue(key);
        const afterGet = await storageLocal.keyValueStore(storeName).get();
        expect(beforeGet.modifiedAt.getTime()).toBe(afterGet.modifiedAt.getTime());
        expect(afterGet.accessedAt.getTime()).toBeGreaterThan(beforeGet.accessedAt.getTime());
    });

    test('listKeys updates accessedAt', async () => {
        const beforeGet = getStats(storeName);
        await storageLocal.keyValueStore(storeName).listKeys();
        const afterGet = await storageLocal.keyValueStore(storeName).get();
        expect(beforeGet.modifiedAt.getTime()).toBe(afterGet.modifiedAt.getTime());
        expect(afterGet.accessedAt.getTime()).toBeGreaterThan(beforeGet.accessedAt.getTime());
    });
});

describe('get store', () => {
    test('returns correct store', async () => {
        let queue = await storageLocal.keyValueStore('first').get();
        expect(queue.id).toBe('first');
        expect(queue.name).toBe('first');
        queue = await storageLocal.keyValueStore('second').get();
        expect(queue.id).toBe('second');
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
        expect(queue.id).toBe('first');
        const count = counter.stores();
        expect(count).toBe(2);
    });

    test('creates a new queue with name', async () => {
        const queueName = 'third';
        const queue = await storageLocal.keyValueStores().getOrCreate(queueName);
        expect(queue.id).toBe('third');
        expect(queue.name).toBe(queueName);
        const count = counter.stores();
        expect(count).toBe(3);
    });
});

describe('delete store', () => {
    test('deletes correct store', async () => {
        await storageLocal.keyValueStore('first').delete();
        const count = counter.stores();
        expect(count).toBe(1);
    });
});

describe('setValue', () => {
    const storeName = 'first';
    const startCount = TEST_STORES[1].recordCount;
    const value = numToRecord(1);

    test('adds a value', async () => { /* eslint-disable no-shadow */
        const record = numToRecord(startCount + 1);

        await storageLocal.keyValueStore(storeName).setValue(record.key, record.data, { contentType: record.contentType });
        expect(counter.records(storeName)).toBe(startCount + 1);

        const recordPath = path.join(storeNameToDir(storeName), record.filename);
        const savedData = await fs.readFile(recordPath);
        expect(savedData).toEqual(Buffer.from(record.data));
    });

    test('updates when key is already present', async () => {
        const newData = Buffer.from('abc');
        await storageLocal.keyValueStore(storeName).setValue(value.key, newData, { contentType: value.contentType });
        expect(counter.records(storeName)).toBe(startCount);
        const recordPath = path.join(storeNameToDir(storeName), value.filename);
        const savedData = await fs.readFile(recordPath);
        expect(savedData).toEqual(newData);
    });

    describe('throws', () => {
        test('when store does not exist', async () => {
            const id = 'non-existent';
            try {
                await storageLocal.keyValueStore(id).setValue('some-key', 'some-value');
                throw new Error('wrong-error');
            } catch (err) {
                expect(err.message).toBe(`Key-value store with id: ${id} does not exist.`);
            }
        });
    });
});

describe('getValue', () => {
    const storeName = 'first';
    const startCount = TEST_STORES[1].recordCount;

    test('gets values', async () => {
        let expectedRecord = numToRecord(3);
        let value = await storageLocal.keyValueStore(storeName).getValue(expectedRecord.key);
        expect(value).toEqual(expectedRecord.data);
        expectedRecord = numToRecord(30);
        value = await storageLocal.keyValueStore('second').getValue(expectedRecord.key);
        expect(value).toEqual(expectedRecord.data);
    });

    test('returns undefined for non-existent records', async () => {
        const expectedRecord = numToRecord(startCount + 1);
        const value = await storageLocal.keyValueStore('first').getValue(expectedRecord.key);
        expect(value).toBeUndefined();
    });
});

describe('listKeys', () => {
    const store = TEST_STORES[2];

    test('fetches keys in correct order', async () => {
        const records = createRecords(store);
        const expectedKeys = records
            .map((r) => ({ key: r.key, size: r.size }))
            .sort((a, b) => {
                if (a.key < b.key) return -1;
                if (a.key > b.key) return 1;
                return 0;
            });

        const { items } = await storageLocal.keyValueStore(store.name).listKeys();
        expect(items).toEqual(expectedKeys);
    });

    test('limit works', async () => {
        const limit = 10;
        const records = createRecords(store);
        const expectedItems = records
            .map((r) => ({ key: r.key, size: r.size }))
            .sort((a, b) => {
                if (a.key < b.key) return -1;
                if (a.key > b.key) return 1;
                return 0;
            })
            .slice(0, limit);

        const { items } = await storageLocal.keyValueStore(store.name).listKeys({ limit });
        expect(items).toEqual(expectedItems);
    });
});

function seed(keyValueStoresDir) {
    Object.values(TEST_STORES).forEach((store) => {
        const storeDir = insertStore(keyValueStoresDir, store);
        const records = createRecords(store);
        insertRecords(storeDir, records);
    });
}

function insertStore(dir, store) {
    const storeDir = path.join(dir, store.name);
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
        const data =`<html><body>${num}: ✅</body></html>`;
        return {
            key,
            data,
            filename: `${key}.html`,
            contentType: 'text/html',
            size: Buffer.byteLength(data),
        };
    }
    if (num % 7 === 0) {
        const key = `buffer_${num}`;
        const chunks = Array(5000).fill(`${num}🚀🎯`);
        const data = Buffer.from(chunks);
        return {
            key,
            data,
            filename: `${key}.bin`,
            contentType: 'application/octet-stream',
            size: data.byteLength,
        };
    }
    const key = `object_${num}`;
    const filename = `${key}.json`;
    const data = JSON.stringify({ number: num, filename });
    return {
        key,
        data,
        filename,
        contentType: 'application/json',
        size: Buffer.byteLength(data),
    };
}

function createCounter(keyValueStoresDir) {
    return {
        stores() {
            return fs.readdirSync(keyValueStoresDir).length;
        },
        records(name) {
            return fs.readdirSync(path.join(keyValueStoresDir, name)).length;
        },
    };
}
