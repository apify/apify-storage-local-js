const fs = require('fs-extra');
const ow = require('ow').default;
const path = require('path');
const RequestQueueEmulator = require('../emulators/request_queue_emulator');
const { purgeNullsFromObject } = require('../utils');

/**
 * Request queue collection client.
 *
 * @property {RequestQueueEmulator} emulator
 */
class RequestQueueCollectionClient {
    /**
     * @param {object} options
     * @param {string} options.storageDir
     * @param {DatabaseConnectionCache} options.dbConnections
     * @param {boolean} options.useWalMode
     */
    constructor(options) {
        const {
            storageDir,
            dbConnections,
            useWalMode,
        } = options;

        this.storageDir = storageDir;
        this.dbConnections = dbConnections;
        this.useWalMode = useWalMode;
    }

    async list() {
        throw new Error('This method is not implemented in @apify/storage-local yet.');
    }

    async getOrCreate(name) {
        ow(name, ow.string.nonEmpty);
        const queueDir = path.join(this.storageDir, name);
        await fs.ensureDir(queueDir);
        const emulator = new RequestQueueEmulator({
            queueDir,
            dbConnections: this.dbConnections,
            useWalMode: this.useWalMode,
        });
        const queue = emulator.selectOrInsertByName(name);
        queue.id = queue.name;
        return purgeNullsFromObject(queue);
    }
}

module.exports = RequestQueueCollectionClient;
