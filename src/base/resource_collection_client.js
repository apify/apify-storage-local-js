const ow = require('ow');
const { purgeNullsFromObject } = require('../utils');

/**
 * Resource collection client.
 */
class ResourceCollectionClient {
    /**
     * @param {object} options
     * @param {DatabaseClient} options.dbClient
     */
    constructor(options) {
        const {
            dbClient,
        } = options;

        this.dbClient = dbClient;
    }

    async list() {
        throw new Error('This method is not implemented in @apify/storage-local yet.');
    }

    async getOrCreate(name) {
        ow(name, ow.optional.string);
        const storage = this.dbClient.selectOrInsertByName(name);
        return purgeNullsFromObject(storage);
    }
}

module.exports = ResourceCollectionClient;
