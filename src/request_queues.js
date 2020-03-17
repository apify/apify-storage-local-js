const ow = require('ow');
const { TIME_FORMAT } = require('./consts');
const { uniqueKeyToRequestId } = require('./utils');

const RESOURCE_TABLE_NAME = 'RequestQueues';
const REQUESTS_TABLE_NAME = 'RequestQueueRequests';

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

class QueueOperationInfo {
    /**
     * @param {string} requestId
     * @param {number|null} [requestOrderNo]
     */
    constructor(requestId, requestOrderNo) {
        this.requestId = requestId;
        this.wasAlreadyPresent = requestOrderNo !== undefined;
        this.wasAlreadyHandled = requestOrderNo === null;
    }
}

class RequestQueues {
    constructor(db) {
        this.db = db;
        this._createTables();
        this._createTriggers();
        this._createIndexes();
        this._prepareStatements();
        this._prepareTransactions();
    }

    /**
     * @param {object} options
     * @param {string} options.queueId
     * @returns {object|null}
     */
    getQueue(options = {}) {
        const { queueId } = options;
        const queue = this.selectQueueById.get(queueId);
        return queue || null;
    }

    /**
     * @param {object} options
     * @param {string} options.queueName
     * @returns {object}
     */
    getOrCreateQueue(options = {}) {
        const { queueName } = options;
        const queue = this.selectQueueByName.get(queueName);
        if (queue) return queue;
        const { lastInsertRowid } = this.insertQueueByName.run(queueName);
        return this.selectQueueById.get(lastInsertRowid);
    }

    /**
     * @param {object} options
     * @param {string} options.queueId
     */
    deleteQueue(options = {}) {
        const { queueId } = options;
        this.deleteQueueById.run(queueId);
    }

    /**
     * @param {object} options
     * @param {string} options.queueId
     * @param {object} options.request
     * @param {boolean} [options.forefront=false]
     * @returns {QueueOperationInfo}
     */
    addRequest(options) {
        ow(options, 'AddRequestOptions', ow.object.partialShape({
            queueId: ow.string,
            request: ow.object.partialShape({
                id: ow.undefined,
                url: ow.string,
                uniqueKey: ow.string,
            }),
            forefront: ow.optional.boolean,
        }));

        const { queueId, request, forefront = false } = options;

        const requestModel = this._createRequestModel(queueId, request, forefront);
        let queueOperationInfo = new QueueOperationInfo(requestModel.id);

        try {
            this.insertRequestByModel.run(requestModel);
        } catch (err) {
            queueOperationInfo = this._handleInsertError(err, requestModel);
        }

        return queueOperationInfo;
    }

    /**
     * @param {object} options
     * @param {string} options.queueId
     * @param {string} options.requestId
     * @returns {object}
     */
    getRequest(options = {}) {
        const { queueId, requestId } = options;
        const json = this.selectRequestJsonByModel.get({
            queueId,
            id: requestId,
        });
        return JSON.parse(json);
    }

    /**
     * @param {object} options
     * @param {string} options.queueId
     * @param {object} options.request
     * @param {boolean} [options.forefront=false]
     * @returns {QueueOperationInfo}
     */
    updateRequest(options = {}) {
        const { queueId, request, forefront = false } = options;
        // if (!request.id) throw new Error('request-id-missing');
        const requestModel = this._createRequestModel(queueId, request, forefront);
        return this.updateRequestTransaction(requestModel);
    }

    /**
     * @param {object} options
     * @param {string} options.queueId
     * @param {number} [options.limit=100]
     * @returns {QueueHead}
     */
    getHead(options = {}) {
        const { queueId, limit = 100 } = options;
        const requestJsons = this.selectRequestJsonsByQueueIdWithLimit.all(queueId, limit);
        const queueModifiedAt = new Date(); // TODO
        return {
            limit,
            queueModifiedAt,
            hadMultipleClients: false,
            items: requestJsons.map(json => JSON.parse(json)),
        };
    }

    /**
     * @param {string} queueId
     * @param {object} request
     * @param {boolean} forefront
     * @returns {RequestModel}
     * @private
     */
    _createRequestModel(queueId, request, forefront) {
        const orderNo = this._calculateOrderNo(request, forefront);
        const id = uniqueKeyToRequestId(request.uniqueKey);
        if (request.id && id !== request.id) throw new Error('request-id-invalid');
        return {
            id,
            queueId,
            orderNo,
            url: request.url,
            uniqueKey: request.uniqueKey,
            method: request.method,
            retryCount: request.retryCount,
            json: JSON.stringify(request),
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

    /**
     * @param {Error} error
     * @param {object} requestModel
     * @returns {QueueOperationInfo}
     * @private
     */
    _handleInsertError(error, requestModel) {
        if (error.message !== 'TODO not unique') throw error;
        const orderNo = this.selectRequestOrderNoByModel.get(requestModel);
        return new QueueOperationInfo(requestModel.id, orderNo);
    }

    _createTables() {
        this.db.prepare(`
            CREATE TABLE IF NOT EXISTS ${RESOURCE_TABLE_NAME}(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                createdAt TEXT DEFAULT(STRFTIME('${TIME_FORMAT}', 'NOW')),
                modifiedAt TEXT,
                accessedAt TEXT,
                totalRequestCount INTEGER DEFAULT 0,
                handledRequestCount INTEGER DEFAULT 0,
                pendingRequestCount INTEGER DEFAULT 0
            )
        `).run();
        this.db.prepare(`
            CREATE TABLE IF NOT EXISTS ${REQUESTS_TABLE_NAME}(
                queueId INTEGER NOT NULL,
                id TEXT NOT NULL,
                orderNo INTEGER,
                url TEXT NOT NULL,
                uniqueKey TEXT NOT NULL,
                method TEXT,
                retryCount INTEGER,
                json TEXT NOT NULL,
                PRIMARY KEY (queueId, id, uniqueKey)
            )
        `).run();
    }

    _createTriggers() {
        this.db.prepare(`
            CREATE TRIGGER IF NOT EXISTS T_update_modifiedAt_on_insert
                AFTER INSERT ON ${REQUESTS_TABLE_NAME}
            BEGIN
                UPDATE ${RESOURCE_TABLE_NAME}
                SET modifiedAt = STRFTIME('${TIME_FORMAT}', 'NOW')
                WHERE id = new.queueId;
            END
        `).run();
        this.db.prepare(`
            CREATE TRIGGER IF NOT EXISTS T_update_modifiedAt_on_update
                AFTER UPDATE ON ${REQUESTS_TABLE_NAME}
            BEGIN
                UPDATE ${RESOURCE_TABLE_NAME}
                SET modifiedAt = STRFTIME('${TIME_FORMAT}', 'NOW')
                WHERE id = NEW.queueId;
            END;
        `).run();
    }

    _createIndexes() {
        this.db.prepare(`
            CREATE INDEX IF NOT EXISTS I_queueId_orderNo
            ON ${REQUESTS_TABLE_NAME}(queueId, orderNo)
            WHERE orderNo IS NOT NULL
        `).run();
    }

    _prepareStatements() {
        this.selectQueueById = this.db.prepare(`
            SELECT *, CAST(id as TEXT) as id
            FROM ${RESOURCE_TABLE_NAME}
            WHERE id = ?
        `);
        this.selectQueueByName = this.db.prepare(`
            SELECT *, CAST(id as TEXT) as id
            FROM ${RESOURCE_TABLE_NAME}
            WHERE name = ?
        `);
        this.insertQueueByName = this.db.prepare(`
            INSERT INTO ${RESOURCE_TABLE_NAME}(name)
            VALUES(?)
        `);
        this.deleteQueueById = this.db.prepare(`
            DELETE FROM ${RESOURCE_TABLE_NAME}
            WHERE id = ?
        `);
        this.insertRequestByModel = this.db.prepare(`
            INSERT INTO ${REQUESTS_TABLE_NAME}(
                id, queueId, orderNo, url, uniqueKey, method, retryCount, json
            ) VALUES (
                :id, CAST(:queueId as INTEGER), :orderNo, :url, :uniqueKey, :method, :retryCount, :json
            )
        `);
        this.selectRequestOrderNoByModel = this.db.prepare(`
            SELECT orderNo FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = CAST(:queueId as INTEGER) AND id = :id
        `).pluck();
        this.selectRequestJsonByModel = this.db.prepare(`
            SELECT json FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = CAST(:queueId as INTEGER) AND id = :id
        `).pluck();
        this.updateRequestByModel = this.db.prepare(`
            UPDATE ${REQUESTS_TABLE_NAME}
            SET orderNo = :orderNo,
                url = :url,
                uniqueKey = :uniqueKey,
                method = :method,
                retryCount = :retryCount,
                json = :json
            WHERE queueId = CAST(:queueId as INTEGER) AND id = :id
        `);
        this.selectRequestJsonsByQueueIdWithLimit = this.db.prepare(`
            SELECT json FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = CAST(? as INTEGER) AND orderNo IS NOT NULL
            LIMIT ?
        `).pluck();
    }

    _prepareTransactions() {
        this.updateRequestTransaction = this.db.transaction((requestModel) => {
            const { changes } = this.updateRequestByModel.run(requestModel);
            // No changes means the request wasn't there yet.
            // We insert it, to behave the same as API.
            if (changes === 0) {
                this.insertRequestByModel.run(requestModel);
                return new QueueOperationInfo(requestModel.id);
            }
            // Now we know the request was there, so we need to
            // check whether it was handled or not.
            const orderNo = this.selectRequestOrderNoByModel.get(requestModel);
            return new QueueOperationInfo(requestModel.id, orderNo);
        });
    }
}
RequestQueues.RESOURCE_TABLE_NAME = RESOURCE_TABLE_NAME;
RequestQueues.REQUESTS_TABLE_NAME = REQUESTS_TABLE_NAME;
module.exports = RequestQueues;
