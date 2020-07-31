const fs = require('fs-extra');
const ow = require('ow');
const path = require('path');
const { STORAGE_NAMES } = require('./consts');
const DatabaseConnectionCache = require('./database_connection_cache');
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
 * Only Request Queue emulation is currently supported.
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

        fs.ensureDirSync(this.requestQueueDir);
        fs.ensureDirSync(this.keyValueStoreDir);
        fs.ensureDirSync(this.datasetDir);
    }

    keyValueStores() {
        return new KeyValueStoreCollectionClient({
            storageDir: this.keyValueStoreDir,
        });
    }

    keyValueStore(id) {
        ow(id, ow.string);
        return new KeyValueStoreClient({
            name: id,
            storageDir: this.keyValueStoreDir,
        });
    }

    requestQueues() {
        return new RequestQueueCollectionClient({
            storageDir: this.requestQueueDir,
            dbConnections: this.dbConnections,
        });
    }

    requestQueue(id) {
        ow(id, ow.string);
        return new RequestQueueClient({
            name: id,
            storageDir: this.requestQueueDir,
            dbConnections: this.dbConnections,
        });
    }
}

module.exports = ApifyStorageLocal;
