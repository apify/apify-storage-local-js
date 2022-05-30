import log from '@apify/log';
import { join, resolve } from 'path';
import { readdirSync } from 'fs';
import { ensureDir, ensureDirSync, statSync, writeFile, writeFileSync } from 'fs-extra';
import { ApifyStorageLocal } from '../src';
import { STORAGE_NAMES } from '../src/consts';
import { prepareTestDir, removeTestDir } from './_tools';

let STORAGE_DIR: string;

beforeEach(() => {
    STORAGE_DIR = prepareTestDir();
});

afterAll(() => {
    removeTestDir(STORAGE_DIR);
});

test('does not create folders immediately', () => {
    // eslint-disable-next-line no-new -- Testing to make sure creating an instance won't immediately create folders
    new ApifyStorageLocal({
        storageDir: STORAGE_DIR,
    });
    const requestQueueDir = join(STORAGE_DIR, STORAGE_NAMES.REQUEST_QUEUES);
    const keyValueStoreDir = join(STORAGE_DIR, STORAGE_NAMES.KEY_VALUE_STORES);
    const datasetDir = join(STORAGE_DIR, STORAGE_NAMES.DATASETS);
    for (const dir of [requestQueueDir, keyValueStoreDir, datasetDir]) {
        expect(() => statSync(dir)).toThrow('ENOENT');
    }
});

test('creates folders lazily + purging', async () => {
    const storage = new ApifyStorageLocal({
        storageDir: STORAGE_DIR,
    });
    const requestQueueDir = join(STORAGE_DIR, STORAGE_NAMES.REQUEST_QUEUES);
    storage.requestQueues();
    const keyValueStoreDir = join(STORAGE_DIR, STORAGE_NAMES.KEY_VALUE_STORES);
    storage.keyValueStores();
    const datasetDir = join(STORAGE_DIR, STORAGE_NAMES.DATASETS);
    storage.datasets();

    for (const dir of [requestQueueDir, keyValueStoreDir, datasetDir]) {
        expect(statSync(dir).isDirectory()).toBe(true);

        const storagePath = resolve(dir, 'default');
        await ensureDir(storagePath);

        const fileData = JSON.stringify({ foo: 'bar' });
        await writeFile(join(storagePath, '000000001.json'), fileData);

        if (dir === keyValueStoreDir) {
            await writeFile(join(storagePath, 'INPUT.json'), fileData);
        }
    }

    await storage.purge();

    // default storages should be empty, except INPUT.json file
    expect(readdirSync(join(datasetDir, 'default')).length).toBe(0);
    expect(readdirSync(join(keyValueStoreDir, 'default')).length).toBe(1);
    expect(readdirSync(join(keyValueStoreDir, 'default'))[0]).toBe('INPUT.json');
    expect(readdirSync(join(requestQueueDir, 'default')).length).toBe(0);
});

test('reads env vars', () => {
    const envVars = { ...process.env };

    process.env = {
        APIFY_LOCAL_STORAGE_DIR: STORAGE_DIR,
        APIFY_LOCAL_STORAGE_ENABLE_WAL_MODE: 'false',
    };

    const storageLocal = new ApifyStorageLocal({
        storageDir: `not_a_${STORAGE_DIR}`,
        enableWalMode: true,
    });

    const requestQueueDir = join(STORAGE_DIR, STORAGE_NAMES.REQUEST_QUEUES);
    storageLocal.requestQueues();
    const keyValueStoreDir = join(STORAGE_DIR, STORAGE_NAMES.KEY_VALUE_STORES);
    storageLocal.keyValueStores();
    const datasetDir = join(STORAGE_DIR, STORAGE_NAMES.DATASETS);
    storageLocal.datasets();
    for (const dir of [requestQueueDir, keyValueStoreDir, datasetDir]) {
        expect(statSync(dir).isDirectory()).toBe(true);
    }
    expect(storageLocal.enableWalMode).toBeFalsy();

    process.env = { ...envVars };
});

test('warning is shown when storage is non-empty', () => {
    const storageLocal = new ApifyStorageLocal({
        storageDir: STORAGE_DIR,
    });

    const requestQueueDir = join(STORAGE_DIR, STORAGE_NAMES.REQUEST_QUEUES);
    const keyValueStoreDir = join(STORAGE_DIR, STORAGE_NAMES.KEY_VALUE_STORES);
    const datasetDir = join(STORAGE_DIR, STORAGE_NAMES.DATASETS);

    const fileData = JSON.stringify({ foo: 'bar' });
    const innerDirName = 'default';

    const innerRequestQueueDir = join(requestQueueDir, innerDirName);
    ensureDirSync(innerRequestQueueDir);
    writeFileSync(join(innerRequestQueueDir, '000000001.json'), fileData);

    const innerKeyValueStoreDir = join(keyValueStoreDir, innerDirName);
    ensureDirSync(innerKeyValueStoreDir);
    writeFileSync(join(innerKeyValueStoreDir, 'INPUT.json'), fileData);

    const innerDatasetDir = join(datasetDir, innerDirName);
    ensureDirSync(innerDatasetDir);
    writeFileSync(join(innerDatasetDir, '000000001.json'), fileData);

    const warnings = jest.spyOn(log, 'warning');

    storageLocal.keyValueStores();
    storageLocal.requestQueues();
    storageLocal.datasets();

    // warning is expected to be shown 2 times only (for Dataset and Request queue)
    // as it should not be shown when INPUT.json in the only file in Key-value store
    expect(warnings).toHaveBeenCalledTimes(2);
});
