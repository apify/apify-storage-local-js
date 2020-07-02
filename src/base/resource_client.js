const ow = require('ow');
const { purgeNullsFromObject } = require('../utils');

/**
 * Resource client.
 */
class ResourceClient {
    /**
     * @param {object} options
     * @param {DatabaseClient} options.dbClient
     * @param {string} options.id
     */
    constructor(options) {
        const {
            dbClient,
            id,
        } = options;

        this.id = id;
        this.dbClient = dbClient;
    }

    async get() {
        const storage = this.dbClient.selectById(this.id);
        return purgeNullsFromObject(storage);
    }

    async update(newFields) {
        ow(newFields, ow.object);
        const fieldNames = Object.keys(newFields);
        const fieldSql = fieldNames.map((name) => `${name} = :${name}`).join(', ');
        this.dbClient.runSql(`
            UPDATE ${this.dbClient.tableName}
            SET ${fieldSql}
            WHERE id = CAST(${this.id} as INTEGER)
        `);
        const storage = this.dbClient.selectById(this.id);
        return purgeNullsFromObject(storage);
    }

    async delete() {
        this.dbClient.deleteById(this.id);
    }
}

module.exports = ResourceClient;
