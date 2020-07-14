const fs = require('fs-extra');
const ow = require('ow');
const path = require('path');
const { STORAGE_NAMES } = require('./consts');
const KeyValueStoreEmulator = require('./emulators/key_value_store_emulator');
const KeyValueStoreClient = require('./resource_clients/key_value_store');
const KeyValueStoreCollectionClient = require('./resource_clients/key_value_store_collection');
const RequestQueueEmulator = require('./emulators/request_queue_emulator');
const RequestQueueClient = require('./resource_clients/request_queue');
const RequestQueueCollectionClient = require('./resource_clients/request_queue_collection');

/**
 * @typedef {object} ApifyStorageLocalOptions
 * @property {string} [storageDir='./apify_storage']
 *  Path to directory where the database files will be created,
 *  unless either the inMemory option is true or the files
 *  already exist.
 * @property {RequestQueueEmulatorOptions} [requestQueueEmulatorOptions]
 *  Options to alter functionality of the request queue database powered
 *  by SQLite.
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
            requestQueueEmulatorOptions: ow.optional.object.exactShape({
                filename: ow.optional.string,
                debug: ow.optional.boolean,
                inMemory: ow.optional.boolean,
            }),
        }));

        const {
            storageDir = './apify_storage',
        } = options;

        this.storageDir = storageDir;
        this.requestQueueDir = path.resolve(storageDir, STORAGE_NAMES.REQUEST_QUEUES);
        this.keyValueStoreDir = path.resolve(storageDir, STORAGE_NAMES.KEY_VALUE_STORES);

        fs.ensureDirSync(this.requestQueueDir);
        fs.ensureDirSync(this.keyValueStoreDir);

        this.requestQueueEmulator = new RequestQueueEmulator(this.requestQueueDir, options.requestQueueEmulatorOptions);
        this.keyValueStoreEmulator = new KeyValueStoreEmulator(this.keyValueStoreDir);
    }

    keyValueStores() {
        return new KeyValueStoreCollectionClient({
            emulator: this.keyValueStoreEmulator,
            storageDir: this.storageDir,
        });
    }

    keyValueStore(id) {
        ow(id, ow.string);
        return new KeyValueStoreClient({
            id,
            emulator: this.keyValueStoreEmulator,
            storageDir: this.storageDir,
        });
    }

    requestQueues() {
        return new RequestQueueCollectionClient({
            emulator: this.requestQueueEmulator,
            storageDir: this.storageDir,
        });
    }

    requestQueue(id) {
        ow(id, ow.string);
        return new RequestQueueClient({
            id,
            emulator: this.requestQueueEmulator,
            storageDir: this.storageDir,
        });
    }
}

module.exports = ApifyStorageLocal;
