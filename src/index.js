const fs = require('fs-extra');
const ow = require('ow').default;
const path = require('path');
const log = require('apify-shared/log');
const { KEY_VALUE_STORE_KEYS } = require('apify-shared/consts');
const { STORAGE_NAMES, STORAGE_TYPES } = require('./consts');
const DatabaseConnectionCache = require('./database_connection_cache');
const DatasetClient = require('./resource_clients/dataset');
const DatasetCollectionClient = require('./resource_clients/dataset_collection');
const KeyValueStoreClient = require('./resource_clients/key_value_store');
const KeyValueStoreCollectionClient = require('./resource_clients/key_value_store_collection');
const RequestQueueClient = require('./resource_clients/request_queue');
const RequestQueueCollectionClient = require('./resource_clients/request_queue_collection');

// Singleton cache to be shared across all ApifyStorageLocal instances
// to make sure that multiple connections are not created to the same database.
const databaseConnectionCache = new DatabaseConnectionCache();

/**
 * @typedef {object} ApifyStorageLocalOptions
 * @property {string} [storageDir='./apify_storage']
 *  Path to directory with storages. If there are no storages yet,
 *  appropriate sub-directories will be created in this directory.
 */

/**
 * Represents local emulation of [Apify Storage](https://apify.com/storage).
 */
class ApifyStorageLocal {
    /**
     * @param {ApifyStorageLocalOptions} [options]
     */
    constructor(options = {}) {
        ow(options, 'ApifyStorageLocalOptions', ow.optional.object.exactShape({
            storageDir: ow.optional.string,
        }));

        const {
            storageDir = './apify_storage',
        } = options;

        this.storageDir = storageDir;
        this.requestQueueDir = path.resolve(storageDir, STORAGE_NAMES.REQUEST_QUEUES);
        this.keyValueStoreDir = path.resolve(storageDir, STORAGE_NAMES.KEY_VALUE_STORES);
        this.datasetDir = path.resolve(storageDir, STORAGE_NAMES.DATASETS);
        this.dbConnections = databaseConnectionCache;

        /**
         * DatasetClient keeps internal state: itemCount
         * We need to keep a single client instance not to
         * have different numbers across parallel clients.
         * @type {Map<string, DatasetClient>}
         */
        this.datasetClientCache = new Map();

        // To prevent directories from being created immediately when
        // an ApifyClient instance is constructed, we create them lazily.
        this.isRequestQueueDirInitialized = false;
        this.isKeyValueStoreDirInitialized = false;
        this.isDatasetDirInitialized = false;
    }

    /**
     * @return {DatasetCollectionClient}
     */
    datasets() {
        this._ensureDatasetDir();
        return new DatasetCollectionClient({
            storageDir: this.datasetDir,
        });
    }

    /**
     * @param {string} id
     * @return {DatasetClient}
     */
    dataset(id) {
        ow(id, ow.string);
        this._ensureDatasetDir();
        let client = this.datasetClientCache.get(id);
        if (!client) {
            client = new DatasetClient({
                name: id,
                storageDir: this.datasetDir,
            });
        }
        return client;
    }

    /**
     * @return {KeyValueStoreCollectionClient}
     */
    keyValueStores() {
        this._ensureKeyValueStoreDir();
        return new KeyValueStoreCollectionClient({
            storageDir: this.keyValueStoreDir,
        });
    }

    /**
     * @param {string} id
     * @return {KeyValueStoreClient}
     */
    keyValueStore(id) {
        ow(id, ow.string);
        this._ensureKeyValueStoreDir();
        return new KeyValueStoreClient({
            name: id,
            storageDir: this.keyValueStoreDir,
        });
    }

    /**
     * @return {RequestQueueCollectionClient}
     */
    requestQueues() {
        this._ensureRequestQueueDir();
        return new RequestQueueCollectionClient({
            storageDir: this.requestQueueDir,
            dbConnections: this.dbConnections,
        });
    }

    /**
     * @param {string} id
     * @param {object} options
     * @return {RequestQueueClient}
     */
    requestQueue(id, options = {}) {
        ow(id, ow.string);
        // Matching the Client validation.
        ow(options, ow.object.exactShape({
            clientKey: ow.optional.string,
        }));
        this._ensureRequestQueueDir();
        return new RequestQueueClient({
            name: id,
            storageDir: this.requestQueueDir,
            dbConnections: this.dbConnections,
        });
    }

    /**
     * @private
     */
    _ensureDatasetDir() {
        if (!this.isDatasetDirInitialized) {
            fs.ensureDirSync(this.datasetDir);
            this._checkIfStorageIsEmpty(STORAGE_TYPES.DATASET, this.datasetDir);
            this.isDatasetDirInitialized = true;
        }
    }

    /**
     * @private
     */
    _ensureKeyValueStoreDir() {
        if (!this.isKeyValueStoreDirInitialized) {
            fs.ensureDirSync(this.keyValueStoreDir);
            this._checkIfStorageIsEmpty(STORAGE_TYPES.KEY_VALUE_STORE, this.keyValueStoreDir);
            this.isKeyValueStoreDirInitialized = true;
        }
    }

    /**
     * @private
     */
    _ensureRequestQueueDir() {
        if (!this.isRequestQueueDirInitialized) {
            fs.ensureDirSync(this.requestQueueDir);
            this._checkIfStorageIsEmpty(STORAGE_TYPES.REQUEST_QUEUE, this.requestQueueDir);
            this.isRequestQueueDirInitialized = true;
        }
    }

    _checkIfStorageIsEmpty(storageType, storageDir) {
        const dirsWithPreviousState = [];

        const dirents = fs.readdirSync(storageDir, { withFileTypes: true });
        for (const dirent of dirents) {
            if (!dirent.isDirectory()) continue; // eslint-disable-line

            const innerStorageDir = path.resolve(storageDir, dirent.name);
            let innerDirents = fs.readdirSync(innerStorageDir).filter((fileName) => !(/(^|\/)\.[^/.]/g).test(fileName));
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

module.exports = ApifyStorageLocal;
