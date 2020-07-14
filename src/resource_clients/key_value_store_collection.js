const ow = require('ow');

/**
 * Key-value store collection client.
 *
 * @property {RequestQueueEmulator} emulator
 */
class KeyValueStoreCollectionClient {
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
}

module.exports = KeyValueStoreCollectionClient;
