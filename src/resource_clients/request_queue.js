const fs = require('fs-extra');
const ow = require('ow');
const path = require('path');
const RequestQueueEmulator = require('../emulators/request_queue_emulator');
const { purgeNullsFromObject, uniqueKeyToRequestId } = require('../utils');

const requestShape = {
    url: ow.string,
    uniqueKey: ow.string,
    method: ow.optional.string,
    retryCount: ow.optional.number,
    handledAt: ow.optional.any(ow.string.date, ow.date),
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
 */
class RequestQueueClient {
    /**
     * @param {object} options
     * @param {string} options.name
     * @param {string} options.storageDir
     * @param {DatabaseConnectionCache} options.dbConnections
     */
    constructor(options) {
        const {
            name,
            storageDir,
            dbConnections,
        } = options;

        // Since queues are represented by folders,
        // each DB only has one queue with ID 1.
        this.id = 1;

        this.name = name;
        this.dbConnections = dbConnections;

        this.queueDir = path.join(storageDir, name);
    }

    /**
     * API client does not make any requests immediately after
     * creation so we simulate this by creating the emulator
     * lazily. The outcome is that an attempt to access a queue
     * that does not exist throws only at the access invocation,
     * which is in line with API client.
     *
     * @return {RequestQueueEmulator}
     * @private
     */
    _getEmulator() {
        if (!this.emulator) {
            this.emulator = new RequestQueueEmulator({
                queueDir: this.queueDir,
                dbConnections: this.dbConnections,
            });
        }
        return this.emulator;
    }

    async get() {
        try {
            this._getEmulator().updateAccessedAtById(this.id);
            const queue = this._getEmulator().selectById(this.id);
            queue.id = queue.name;
            return purgeNullsFromObject(queue);
        } catch (err) {
            if (err.code !== 'ENOENT') throw err;
        }
    }

    async update(newFields) {
        // The validation is intentionally loose to prevent issues
        // when swapping to a remote queue in production.
        ow(newFields, ow.object.partialShape({
            name: ow.optional.string.nonEmpty,
        }));
        if (!newFields.name) return;

        const newPath = path.join(path.dirname(this.queueDir), newFields.name);

        // To prevent chaos, we close the database connection before moving the folder.
        this._getEmulator().disconnect();

        try {
            await fs.move(this.queueDir, newPath);
        } catch (err) {
            if (/dest already exists/.test(err.message)) {
                throw new Error('Request queue name is not unique.');
            }
            throw err;
        }

        this.name = newFields.name;

        this._getEmulator().updateNameById(this.id, newFields.name);
        this._getEmulator().updateModifiedAtById(this.id);
        const queue = this._getEmulator().selectById(this.id);
        queue.id = queue.name;
        return purgeNullsFromObject(queue);
    }

    async delete() {
        this._getEmulator().disconnect();

        await fs.remove(this.queueDir);
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

        this._getEmulator().updateAccessedAtById(this.id);
        const requestJsons = this._getEmulator().selectRequestJsonsByQueueIdWithLimit(this.id, limit);
        const queueModifiedAt = this._getEmulator().selectModifiedAtById(this.id);
        return {
            limit,
            queueModifiedAt,
            hadMultipleClients: false,
            items: requestJsons.map((json) => this._jsonToRequest(json)),
        };
    }

    /**
     * @param {object} request
     * @param {object} [options]
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
        return this._getEmulator().addRequest(requestModel);
    }

    /**
     * @param {string} id
     * @returns {Promise<?object>}
     */
    async getRequest(id) {
        ow(id, ow.string);
        this._getEmulator().updateAccessedAtById(this.id);
        const json = this._getEmulator().selectRequestJsonByIdAndQueueId(id, this.id);
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
        return this._getEmulator().updateRequest(requestModel);
    }

    async deleteRequest() {
        // TODO Deletion is done, but we also need to update request counts in a transaction.
        throw new Error('This method is not implemented in @apify/storage-local yet.');
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
