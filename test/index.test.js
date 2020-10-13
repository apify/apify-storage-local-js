const fs = require('fs-extra');
const path = require('path');
const ApifyStorageLocal = require('../src/index');
const { STORAGE_NAMES } = require('../src/consts');
const { prepareTestDir, removeTestDir } = require('./_tools');

let STORAGE_DIR;
beforeEach(() => {
    STORAGE_DIR = prepareTestDir();
});

afterAll(() => {
    removeTestDir(STORAGE_DIR);
});

test('does not create folders immediately', () => {
    const storageLocal = new ApifyStorageLocal({ // eslint-disable-line
        storageDir: STORAGE_DIR,
    });
    const requestQueueDir = path.join(STORAGE_DIR, STORAGE_NAMES.REQUEST_QUEUES);
    const keyValueStoreDir = path.join(STORAGE_DIR, STORAGE_NAMES.KEY_VALUE_STORES);
    const datasetDir = path.join(STORAGE_DIR, STORAGE_NAMES.DATASETS);
    for (const dir of [requestQueueDir, keyValueStoreDir, datasetDir]) {
        expect(() => fs.statSync(dir)).toThrow('ENOENT');
    }
});

test('creates folders lazily', () => {
    const storageLocal = new ApifyStorageLocal({ // eslint-disable-line
        storageDir: STORAGE_DIR,
    });
    const requestQueueDir = path.join(STORAGE_DIR, STORAGE_NAMES.REQUEST_QUEUES);
    storageLocal.requestQueues();
    const keyValueStoreDir = path.join(STORAGE_DIR, STORAGE_NAMES.KEY_VALUE_STORES);
    storageLocal.keyValueStores();
    const datasetDir = path.join(STORAGE_DIR, STORAGE_NAMES.DATASETS);
    storageLocal.datasets();
    for (const dir of [requestQueueDir, keyValueStoreDir, datasetDir]) {
        expect(fs.statSync(dir).isDirectory()).toBe(true);
    }
});
