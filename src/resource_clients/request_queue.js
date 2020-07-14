const ow = require('ow');
const { purgeNullsFromObject, uniqueKeyToRequestId } = require('../utils');

const requestShape = {
    url: ow.string,
    uniqueKey: ow.string,
    method: ow.optional.string,
    retryCount: ow.optional.number,
    handledAt: ow.any(ow.string.date, ow.date, ow.undefined),
};

/**
 * @typedef {object} QueueHead
 * @property {number} limit Maximum number of items to be returned.
 * @property {Date} queueModifiedAt Date of the last modification of the queue.
 * @property {boolean} hadMultipleClients This is always false for local queue.
 * @property {Object[]} items Array of request-like objects.
 */

/**
 * @typedef {object} RequestModel
 * @property {string} id,
 * @property {string} queueId,
 * @property {number|null} orderNo,
 * @property {string} url,
 * @property {string} uniqueKey,
 * @property {?string} method,
 * @property {?number} retryCount,
 * @property {string} json
 */

/**
 * Request queue client.
 *
 * @property {RequestQueueEmulator} emulator
 */
class RequestQueueClient {
    /**
     * @param {object} options
     * @param {RequestQueueEmulator} options.emulator
     * @param {string} options.id
     */
    constructor(options) {
        const {
            emulator,
            id,
        } = options;

        this.id = id;
        this.emulator = emulator;
    }

    async get() {
        this.emulator.updateAccessedAtById(this.id);
        const storage = this.emulator.selectById(this.id);
        return purgeNullsFromObject(storage);
    }

    async update(newFields) {
        ow(newFields, ow.object);
        const fieldNames = Object.keys(newFields);
        const fieldSql = fieldNames.map((name) => `${name} = :${name}`).join(', ');
        this.emulator.runSql(`
            UPDATE ${this.emulator.queueTableName}
            SET ${fieldSql}
            WHERE id = CAST(${this.id} as INTEGER)
        `);
        this.emulator.updateModifiedAtById(this.id);
        const storage = this.emulator.selectById(this.id);
        return purgeNullsFromObject(storage);
    }

    async delete() {
        this.emulator.deleteById(this.id);
    }

    /**
     * @param {object} options
     * @param {number} [options.limit=100]
     * @return {Promise<QueueHead>}
     */
    async listHead(options = {}) {
        ow(options, ow.object.exactShape({
            limit: ow.optional.number,
        }));
        const {
            limit = 100,
        } = options;

        this.emulator.updateAccessedAtById(this.id);
        const requestJsons = this.emulator.selectRequestJsonsByQueueIdWithLimit(this.id, limit);
        const queueModifiedAt = this.emulator.selectModifiedAtById(this.id);
        return {
            limit,
            queueModifiedAt,
            hadMultipleClients: false,
            items: requestJsons.map((json) => this._jsonToRequest(json)),
        };
    }

    /**
     * @param {object} request
     * @param {object} options
     * @param {boolean} [options.forefront]
     * @returns {Promise<QueueOperationInfo>}
     */
    async addRequest(request, options = {}) {
        ow(request, ow.object.partialShape({
            id: ow.undefined,
            ...requestShape,
        }));
        ow(options, ow.object.exactShape({
            forefront: ow.optional.boolean,
        }));

        const requestModel = this._createRequestModel(request, options.forefront);
        return this.emulator.addRequest(requestModel);
    }

    /**
     * @param {string} id
     * @returns {Promise<?object>}
     */
    async getRequest(id) {
        ow(id, ow.string);
        this.emulator.updateAccessedAtById(this.id);
        const json = this.emulator.selectRequestJsonByIdAndQueueId(id, this.id);
        return this._jsonToRequest(json);
    }

    /**
     * @param {object} request
     * @param {string} request.id
     * @param {object} [options]
     * @param {boolean} [options.forefront]
     * @returns {Promise<QueueOperationInfo>}
     */
    async updateRequest(request, options = {}) {
        ow(request, ow.object.partialShape({
            id: ow.string,
            ...requestShape,
        }));
        ow(options, ow.object.exactShape({
            forefront: ow.optional.boolean,
        }));

        const requestModel = this._createRequestModel(request, options.forefront);
        return this.emulator.updateRequest(requestModel);
    }

    async deleteRequest(id) {
        ow(id, ow.string);
        this.emulator.deleteById(id);
    }

    /**
     * @param {object} request
     * @param {boolean} forefront
     * @returns {RequestModel}
     * @private
     */
    _createRequestModel(request, forefront) {
        const orderNo = this._calculateOrderNo(request, forefront);
        const id = uniqueKeyToRequestId(request.uniqueKey);
        if (request.id && id !== request.id) throw new Error('Request ID does not match its uniqueKey.');
        const json = JSON.stringify({ ...request, id });
        return {
            id,
            queueId: this.id,
            orderNo,
            url: request.url,
            uniqueKey: request.uniqueKey,
            method: request.method,
            retryCount: request.retryCount,
            json,
        };
    }

    /**
     * A partial index on the requests table ensures
     * that NULL values are not returned when querying
     * for queue head.
     *
     * @param {object} request
     * @param {boolean} forefront
     * @returns {null|number}
     * @private
     */
    _calculateOrderNo(request, forefront) {
        if (request.handledAt) return null;
        const timestamp = Date.now();
        return forefront ? -timestamp : timestamp;
    }

    _jsonToRequest(requestJson) {
        if (!requestJson) return;
        const request = JSON.parse(requestJson);
        return purgeNullsFromObject(request);
    }
}

module.exports = RequestQueueClient;
