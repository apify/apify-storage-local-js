import { ensureDirSync, pathExists, pathExistsSync, readdirSync } from 'fs-extra';
import ow from 'ow';
import { readdir, rm } from 'fs/promises';
import { join, resolve } from 'path';
import log from '@apify/log';
import { ENV_VARS, KEY_VALUE_STORE_KEYS, LOCAL_ENV_VARS } from '@apify/consts';
import { STORAGE_NAMES, STORAGE_TYPES } from './consts';
import { DatabaseConnectionCache } from './database_connection_cache';
import { DatasetClient } from './resource_clients/dataset';
import { DatasetCollectionClient } from './resource_clients/dataset_collection';
import { KeyValueStoreClient } from './resource_clients/key_value_store';
import { KeyValueStoreCollectionClient } from './resource_clients/key_value_store_collection';
import { RequestQueueClient } from './resource_clients/request_queue';
import { RequestQueueCollectionClient } from './resource_clients/request_queue_collection';

// Singleton cache to be shared across all ApifyStorageLocal instances
// to make sure that multiple connections are not created to the same database.
const databaseConnectionCache = new DatabaseConnectionCache();

export interface ApifyStorageLocalOptions {
    /**
     * Path to directory with storages. If there are no storages yet,
     * appropriate sub-directories will be created in this directory.
     * @default './apify_storage'
     */
    storageDir?: string;

    /**
     * SQLite WAL mode (instead of a rollback journal) is used by default for request queues, however, in some file systems it could behave weirdly.
     * Setting this property to `false` will force the request queue database to use a rollback journal instead of WAL.
     * @default true
     */
    enableWalMode?: boolean;
}

export interface RequestQueueOptions {
    clientKey?: string;
    timeoutSecs?: number;
}

/**
 * Represents local emulation of [Apify Storage](https://apify.com/storage).
 */
export class ApifyStorageLocal {
    readonly storageDir: string;

    readonly requestQueueDir: string;

    readonly keyValueStoreDir: string;

    readonly datasetDir: string;

    readonly dbConnections = databaseConnectionCache;

    readonly enableWalMode: boolean;

    /**
     * DatasetClient keeps internal state: itemCount
     * We need to keep a single client instance not to
     * have different numbers across parallel clients.
     */
    readonly datasetClientCache = new Map<string, DatasetClient>();

    // To prevent directories from being created immediately when
    // an ApifyClient instance is constructed, we create them lazily.
    private isRequestQueueDirInitialized = false;

    private isKeyValueStoreDirInitialized = false;

    private isDatasetDirInitialized = false;

    constructor(options: ApifyStorageLocalOptions = {}) {
        ow(options, 'ApifyStorageLocalOptions', ow.optional.object.exactShape({
            storageDir: ow.optional.string,
            enableWalMode: ow.optional.boolean,
        }));

        /**
         * Returns the first argument which is not `undefined`.
         * If all the arguments are `undefined`, returns `undefined` (but the type is `never`).
         */
        const bool = (val?: string) => (val == null ? val : !['false', '0', ''].includes(val.toLowerCase()));
        const storageDir = process.env.APIFY_LOCAL_STORAGE_DIR ?? options.storageDir ?? './apify_storage';
        const enableWalMode = bool(process.env.APIFY_LOCAL_STORAGE_ENABLE_WAL_MODE) ?? options.enableWalMode ?? true;

        if (!pathExistsSync(storageDir)) {
            log.info(`Created a data storage folder at ${storageDir}. You can override the path by setting the APIFY_LOCAL_STORAGE_DIR environment variable`);
            ensureDirSync(storageDir);
        }

        this.storageDir = storageDir;
        this.requestQueueDir = resolve(storageDir, STORAGE_NAMES.REQUEST_QUEUES);
        this.keyValueStoreDir = resolve(storageDir, STORAGE_NAMES.KEY_VALUE_STORES);
        this.datasetDir = resolve(storageDir, STORAGE_NAMES.DATASETS);
        this.enableWalMode = enableWalMode;

        this.dbConnections.setWalMode(this.enableWalMode);
    }

    datasets(): DatasetCollectionClient {
        this._ensureDatasetDir();
        return new DatasetCollectionClient({
            storageDir: this.datasetDir,
        });
    }

    dataset(id: string): DatasetClient {
        ow(id, ow.string);
        this._ensureDatasetDir();
        let client = this.datasetClientCache.get(id);
        if (!client) {
            client = new DatasetClient({
                name: id,
                storageDir: this.datasetDir,
            });
            this.datasetClientCache.set(id, client);
        }
        return client;
    }

    keyValueStores(): KeyValueStoreCollectionClient {
        this._ensureKeyValueStoreDir();
        return new KeyValueStoreCollectionClient({
            storageDir: this.keyValueStoreDir,
        });
    }

    keyValueStore(id: string): KeyValueStoreClient {
        ow(id, ow.string);
        this._ensureKeyValueStoreDir();
        return new KeyValueStoreClient({
            name: id,
            storageDir: this.keyValueStoreDir,
        });
    }

    requestQueues(): RequestQueueCollectionClient {
        this._ensureRequestQueueDir();
        return new RequestQueueCollectionClient({
            storageDir: this.requestQueueDir,
            dbConnections: this.dbConnections,
        });
    }

    requestQueue(id: string, options: RequestQueueOptions = {}): RequestQueueClient {
        ow(id, ow.string);
        // Matching the Client validation.
        ow(options, ow.object.exactShape({
            clientKey: ow.optional.string,
            timeoutSecs: ow.optional.number,
        }));
        this._ensureRequestQueueDir();
        return new RequestQueueClient({
            name: id,
            storageDir: this.requestQueueDir,
            dbConnections: this.dbConnections,
        });
    }

    /**
     * Cleans up the default local storage directories before the run starts:
     *  - local directory containing the default dataset;
     *  - all records from the default key-value store in the local directory, except for the "INPUT" key;
     *  - local directory containing the default request queue.
     */
    async purge(): Promise<void> {
        const defaultDatasetPath = resolve(this.datasetDir, LOCAL_ENV_VARS[ENV_VARS.DEFAULT_DATASET_ID]);
        await this.removeFiles(defaultDatasetPath);

        const defaultKeyValueStorePath = resolve(this.keyValueStoreDir, LOCAL_ENV_VARS[ENV_VARS.DEFAULT_KEY_VALUE_STORE_ID]);
        await this.removeFiles(defaultKeyValueStorePath);

        const defaultRequestQueuePath = resolve(this.requestQueueDir, LOCAL_ENV_VARS[ENV_VARS.DEFAULT_REQUEST_QUEUE_ID]);
        await this.removeFiles(defaultRequestQueuePath);
    }

    private async removeFiles(folder: string): Promise<void> {
        const storagePathExists = await pathExists(folder);

        if (storagePathExists) {
            const direntNames = await readdir(folder);
            const deletePromises = [];

            for (const direntName of direntNames) {
                const fileName = join(folder, direntName);

                if (!new RegExp(KEY_VALUE_STORE_KEYS.INPUT).test(fileName)) {
                    deletePromises.push(rm(fileName, { recursive: true, force: true }));
                }
            }

            await Promise.all(deletePromises);
        }
    }

    private _ensureDatasetDir() {
        if (!this.isDatasetDirInitialized) {
            ensureDirSync(this.datasetDir);
            this._checkIfStorageIsEmpty(STORAGE_TYPES.DATASET, this.datasetDir);
            this.isDatasetDirInitialized = true;
        }
    }

    private _ensureKeyValueStoreDir() {
        if (!this.isKeyValueStoreDirInitialized) {
            ensureDirSync(this.keyValueStoreDir);
            this._checkIfStorageIsEmpty(STORAGE_TYPES.KEY_VALUE_STORE, this.keyValueStoreDir);
            this.isKeyValueStoreDirInitialized = true;
        }
    }

    private _ensureRequestQueueDir() {
        if (!this.isRequestQueueDirInitialized) {
            ensureDirSync(this.requestQueueDir);
            this._checkIfStorageIsEmpty(STORAGE_TYPES.REQUEST_QUEUE, this.requestQueueDir);
            this.isRequestQueueDirInitialized = true;
        }
    }

    private _checkIfStorageIsEmpty(storageType: STORAGE_TYPES, storageDir: string) {
        const dirsWithPreviousState = [];

        const dirents = readdirSync(storageDir, { withFileTypes: true });
        for (const dirent of dirents) {
            if (!dirent.isDirectory()) continue;

            const innerStorageDir = resolve(storageDir, dirent.name);

            let innerDirents = readdirSync(innerStorageDir).filter((fileName) => !(/(^|\/)\.[^/.]/g).test(fileName));

            if (storageType === STORAGE_TYPES.KEY_VALUE_STORE) {
                innerDirents = innerDirents.filter((fileName) => !RegExp(KEY_VALUE_STORE_KEYS.INPUT).test(fileName));
            }

            if (innerDirents.length) {
                dirsWithPreviousState.push(innerStorageDir);
            }
        }

        const dirsNo = dirsWithPreviousState.length;
        if (dirsNo) {
            log.warning(`The following ${storageType} director${dirsNo === 1 ? 'y' : 'ies'} contain${dirsNo === 1 ? 's' : ''} a previous state:`
                + `\n      ${dirsWithPreviousState.join('\n      ')}`
                + '\n      If you did not intend to persist the state - '
                + `please clear the respective director${dirsNo === 1 ? 'y' : 'ies'} and re-start the actor.`);
        }
    }
}
