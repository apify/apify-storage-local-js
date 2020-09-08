const fs = require('fs-extra');
const ow = require('ow');
const path = require('path');

/**
 * Key-value store collection client.
 *
 * @property {RequestQueueEmulator} emulator
 */
class KeyValueStoreCollectionClient {
    /**
     * @param {object} options
     * @param {string} options.storageDir
     */
    constructor(options) {
        const {
            storageDir,
        } = options;

        this.storageDir = storageDir;
    }

    async list() {
        throw new Error('This method is not implemented in @apify/storage-local yet.');
    }

    async getOrCreate(name) {
        ow(name, ow.string.nonEmpty);
        const storePath = path.join(this.storageDir, name);
        await fs.ensureDir(storePath);
        const stats = await fs.stat(storePath);
        return {
            id: name,
            name,
            createdAt: stats.birthtime,
            modifiedAt: stats.mtime,
            accessedAt: stats.atime,
        };
    }
}

module.exports = KeyValueStoreCollectionClient;
