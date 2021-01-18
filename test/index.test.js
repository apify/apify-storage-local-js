const fs = require('fs-extra');
const path = require('path');
const log = require('apify-shared/log');
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
    const storageLocal = new ApifyStorageLocal({
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

test('warning is shown when storage is non-empty', () => {
    const storageLocal = new ApifyStorageLocal({
        storageDir: STORAGE_DIR,
    });

    const requestQueueDir = path.join(STORAGE_DIR, STORAGE_NAMES.REQUEST_QUEUES);
    const keyValueStoreDir = path.join(STORAGE_DIR, STORAGE_NAMES.KEY_VALUE_STORES);
    const datasetDir = path.join(STORAGE_DIR, STORAGE_NAMES.DATASETS);

    const fileData = JSON.stringify({ foo: 'bar' });
    const innerDirName = 'default';

    const innerRequestQueueDir = path.join(requestQueueDir, innerDirName);
    fs.ensureDirSync(innerRequestQueueDir);
    fs.writeFileSync(path.join(innerRequestQueueDir, '000000001.json'), fileData);

    const innerKeyValueStoreDir = path.join(keyValueStoreDir, innerDirName);
    fs.ensureDirSync(innerKeyValueStoreDir);
    fs.writeFileSync(path.join(innerKeyValueStoreDir, 'INPUT.json'), fileData);

    const innerDatasetDir = path.join(datasetDir, innerDirName);
    fs.ensureDirSync(innerDatasetDir);
    fs.writeFileSync(path.join(innerDatasetDir, '000000001.json'), fileData);

    const warnings = jest.spyOn(log, 'warning');

    storageLocal.keyValueStores();
    storageLocal.requestQueues();
    storageLocal.datasets();

    // warning is expected to be shown 2 times only (for Dataset and Request queue)
    // as it should not be shown when INPUT.json in the only file in Key-value store
    expect(warnings).toHaveBeenCalledTimes(2);
});
