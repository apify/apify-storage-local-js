const ow = require('ow');
const { TIMESTAMP_SQL } = require('./consts');
const { uniqueKeyToRequestId } = require('./utils');

const RESOURCE_TABLE_NAME = 'RequestQueues';
const REQUESTS_TABLE_NAME = 'RequestQueueRequests';

const ERROR_REQUEST_NOT_UNIQUE = 'SQLITE_CONSTRAINT_PRIMARYKEY';
const ERROR_QUEUE_DOES_NOT_EXIST = 'SQLITE_CONSTRAINT_FOREIGNKEY';

const REQUEST_TYPES = {
    url: ow.string,
    uniqueKey: ow.string,
    method: ow.optional.string,
    retryCount: ow.optional.number,
    handledAt: ow.any(ow.string.date, ow.date, ow.nullOrUndefined),
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
    async getQueue(options) {
        ow(options, 'getQueue', ow.object.partialShape({
            queueId: ow.string,
        }));
        const { queueId } = options;
        const queue = this.selectQueueById.get(queueId);
        return queue || null;
    }

    /**
     * @param {object} [options]
     * @param {string} [options.queueName]
     * @returns {Promise<object>}
     */
    async getOrCreateQueue(options) {
        ow(options, 'getOrCreateQueue', ow.optional.object.partialShape({
            queueName: ow.optional.string,
        }));
        const { queueName } = options;
        if (queueName) {
            const queue = this.selectQueueByName.get(queueName);
            if (queue) return queue;
        }

        const { lastInsertRowid } = this.insertQueueByName.run(queueName);
        return this.selectQueueById.get(lastInsertRowid);
    }

    /**
     * @param {object} options
     * @param {string} options.queueId
     * @returns {Promise<object>}
     */
    async deleteQueue(options) {
        ow(options, 'deleteQueue', ow.object.partialShape({
            queueId: ow.string,
        }));
        const { queueId } = options;
        this.deleteQueueById.run(queueId);
    }

    /**
     * @param {object} options
     * @param {string} options.queueId
     * @param {object} options.request
     * @param {boolean} [options.forefront=false]
     * @returns {Promise<QueueOperationInfo>}
     */
    async addRequest(options) {
        ow(options, 'addRequest', ow.object.partialShape({
            queueId: ow.string,
            request: ow.object.partialShape({
                id: ow.undefined, // <- this is different in updateRequest
                ...REQUEST_TYPES,
            }),
            forefront: ow.optional.boolean,
        }));

        const { queueId, request, forefront = false } = options;
        const requestModel = this._createRequestModel(queueId, request, forefront);
        return this.addRequestTransaction(requestModel);
    }

    /**
     * @param {object} options
     * @param {string} options.queueId
     * @param {string} options.requestId
     * @returns {Promise<object>}
     */
    async getRequest(options) {
        ow(options, 'getRequest', ow.object.partialShape({
            queueId: ow.string,
            requestId: ow.string,
        }));
        const { queueId, requestId } = options;
        this.updateQueueAccessedAtById.run(queueId);
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
     * @returns {Promise<QueueOperationInfo>}
     */
    async updateRequest(options) {
        ow(options, 'addRequest', ow.object.partialShape({
            queueId: ow.string,
            request: ow.object.partialShape({
                id: ow.string, // <- this is different in addRequest
                ...REQUEST_TYPES,
            }),
            forefront: ow.optional.boolean,
        }));
        const { queueId, request, forefront = false } = options;
        const requestModel = this._createRequestModel(queueId, request, forefront);
        return this.updateRequestTransaction(requestModel);
    }

    /**
     * @param {object} options
     * @param {string} options.queueId
     * @param {number} [options.limit=100]
     * @returns {Promise<QueueHead>}
     */
    async getHead(options) {
        ow(options, 'getHead', ow.object.partialShape({
            queueId: ow.string,
            limit: ow.optional.number,
        }));
        const { queueId, limit = 100 } = options;
        this.updateQueueAccessedAtById.run(queueId);
        const requestJsons = this.selectRequestJsonsByQueueIdWithLimit.all(queueId, limit);
        const queueModifiedAt = this.selectQueueModifiedAtById.get(queueId);
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
        if (request.id && id !== request.id) throw new Error('Request ID does not match its uniqueKey.');
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

    _createTables() {
        this.db.prepare(`
            CREATE TABLE IF NOT EXISTS ${RESOURCE_TABLE_NAME}(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                createdAt TEXT DEFAULT(${TIMESTAMP_SQL}),
                modifiedAt TEXT,
                accessedAt TEXT,
                totalRequestCount INTEGER DEFAULT 0,
                handledRequestCount INTEGER DEFAULT 0,
                pendingRequestCount INTEGER GENERATED ALWAYS AS (totalRequestCount - handledRequestCount) VIRTUAL
            )
        `).run();
        this.db.prepare(`
            CREATE TABLE IF NOT EXISTS ${REQUESTS_TABLE_NAME}(
                queueId INTEGER NOT NULL REFERENCES ${RESOURCE_TABLE_NAME}(id) ON DELETE CASCADE,
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
        const getSqlForCommand = cmd => `
        CREATE TRIGGER IF NOT EXISTS T_update_modifiedAt_on_${cmd.toLowerCase()}
                AFTER ${cmd} ON ${REQUESTS_TABLE_NAME}
            BEGIN
                UPDATE ${RESOURCE_TABLE_NAME}
                SET modifiedAt = ${TIMESTAMP_SQL},
                    accessedAt = ${TIMESTAMP_SQL}
                WHERE id = ${cmd === 'DELETE' ? 'OLD' : 'NEW'}.queueId;
            END
        `;

        ['INSERT', 'UPDATE', 'DELETE'].forEach((cmd) => {
            const sql = getSqlForCommand(cmd);
            this.db.exec(sql);
        });
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
        this.selectQueueModifiedAtById = this.db.prepare(`
            SELECT modifiedAt FROM ${RESOURCE_TABLE_NAME}
            WHERE id = ?
        `).pluck();
        this.insertQueueByName = this.db.prepare(`
            INSERT INTO ${RESOURCE_TABLE_NAME}(name)
            VALUES(?)
        `);
        this.deleteQueueById = this.db.prepare(`
            DELETE FROM ${RESOURCE_TABLE_NAME}
            WHERE id = CAST(? as INTEGER)
        `);
        this.adjustTotalAndHandledRequestCountsById = this.db.prepare(`
            UPDATE ${RESOURCE_TABLE_NAME}
            SET totalRequestCount = totalRequestCount + :totalAdjustment,
                handledRequestCount = handledRequestCount + :handledAdjustment
            WHERE id = CAST(:queueId as INTEGER)
        `);
        this.updateQueueAccessedAtById = this.db.prepare(`
            UPDATE ${RESOURCE_TABLE_NAME}
            SET accessedAt = ${TIMESTAMP_SQL}
            WHERE id = CAST(? as INTEGER)
        `);
        this.selectRequestOrderNoByModel = this.db.prepare(`
            SELECT orderNo FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = CAST(:queueId as INTEGER) AND id = :id
        `).pluck();
        this.selectRequestJsonByModel = this.db.prepare(`
            SELECT json FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = CAST(:queueId as INTEGER) AND id = :id
        `).pluck();
        this.selectRequestJsonsByQueueIdWithLimit = this.db.prepare(`
            SELECT json FROM ${REQUESTS_TABLE_NAME}
            WHERE queueId = CAST(? as INTEGER) AND orderNo IS NOT NULL
            LIMIT ?
        `).pluck();
        this.insertRequestByModel = this.db.prepare(`
            INSERT INTO ${REQUESTS_TABLE_NAME}(
                id, queueId, orderNo, url, uniqueKey, method, retryCount, json
            ) VALUES (
                :id, CAST(:queueId as INTEGER), :orderNo, :url, :uniqueKey, :method, :retryCount, :json
            )
        `);
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
    }

    _prepareTransactions() {
        this.addRequestTransaction = this.db.transaction((requestModel) => {
            try {
                this.insertRequestByModel.run(requestModel);
                const handledCountAdjustment = requestModel.orderNo === null ? 1 : 0;
                this._adjustRequestCounts(requestModel.queueId, 1, handledCountAdjustment);
                // We return wasAlreadyHandled: false even though the request may
                // have been added as handled, because that's how API behaves.
                return new QueueOperationInfo(requestModel.id);
            } catch (err) {
                if (err.code === ERROR_REQUEST_NOT_UNIQUE) {
                    // If we got here it means that the request was already present.
                    // We need to figure out if it were handled too.
                    const orderNo = this.selectRequestOrderNoByModel.get(requestModel);
                    return new QueueOperationInfo(requestModel.id, orderNo);
                }
                if (err.code === ERROR_QUEUE_DOES_NOT_EXIST) {
                    throw new Error(`Request queue with id: ${requestModel.queueId} does not exist.`);
                }
                throw err;
            }
        });
        this.updateRequestTransaction = this.db.transaction((requestModel) => {
            // First we need to check the existing request to be
            // able to return information about its handled state.
            const orderNo = this.selectRequestOrderNoByModel.get(requestModel);

            // Undefined means that the request is not present in the queue.
            // We need to insert it, to behave the same as API.
            if (orderNo === undefined) {
                return this.addRequestTransaction(requestModel);
            }

            // When updating the request, we need to make sure that
            // the handled counts are updated correctly in all cases.
            this.updateRequestByModel.run(requestModel);
            let handledCountAdjustment = 0;
            const isRequestHandledStateChanging = typeof orderNo !== typeof requestModel.orderNo;
            const requestWasHandledBeforeUpdate = orderNo === null;

            if (isRequestHandledStateChanging) handledCountAdjustment += 1;
            if (requestWasHandledBeforeUpdate) handledCountAdjustment = -handledCountAdjustment;
            this._adjustRequestCounts(requestModel.queueId, 0, handledCountAdjustment);

            // Again, it's important to return the state of the previous
            // request, not the new one, because that's how API does it.
            return new QueueOperationInfo(requestModel.id, orderNo);
        });
    }

    /**
     * Exists to document and simplify the API of the SQL statement
     * @param {string} queueId
     * @param {number} totalAdjustment
     * @param {number} handledAdjustment
     * @private
     */
    _adjustRequestCounts(queueId, totalAdjustment, handledAdjustment) {
        this.adjustTotalAndHandledRequestCountsById.run({
            queueId,
            totalAdjustment,
            handledAdjustment,
        });
    }
}
RequestQueues.RESOURCE_TABLE_NAME = RESOURCE_TABLE_NAME;
RequestQueues.REQUESTS_TABLE_NAME = REQUESTS_TABLE_NAME;
module.exports = RequestQueues;
