const ow = require('ow');
const { purgeNullsFromObject } = require('../utils');

/**
 * Request queue collection client.
 *
 * @property {RequestQueueEmulator} emulator
 */
class RequestQueueCollectionClient {
    /**
     * @param {object} options
     * @param {RequestQueueEmulator} options.emulator
     */
    constructor(options) {
        const {
            emulator,
        } = options;

        this.emulator = emulator;
    }

    async list() {
        throw new Error('This method is not implemented in @apify/storage-local yet.');
    }

    async getOrCreate(name) {
        ow(name, ow.optional.string);
        const storage = this.emulator.selectOrInsertByName(name);
        return purgeNullsFromObject(storage);
    }
}

module.exports = RequestQueueCollectionClient;
