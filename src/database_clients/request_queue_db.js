const DatabaseClient = require('../base/database_client');
const QueueOperationInfo = require('./queue_operation_info');
const { TABLE_NAMES, TIMESTAMP_SQL } = require('../consts');

const ERROR_REQUEST_NOT_UNIQUE = 'SQLITE_CONSTRAINT_PRIMARYKEY';
const ERROR_QUEUE_DOES_NOT_EXIST = 'SQLITE_CONSTRAINT_FOREIGNKEY';

class RequestQueueDatabaseClient extends DatabaseClient {
    constructor(database) {
        super(database, TABLE_NAMES.REQUEST_QUEUES);

        this._createTables();
        this._createTriggers();
        this._createIndexes();
    }

    /**
     * @param {string} id
     * @return {*}
     */
    selectModifiedAtById(id) {
        if (!this._selectModifiedAtById) {
            this._selectModifiedAtById = this.db.prepare(`
                SELECT modifiedAt FROM ${this.tableName}
                WHERE id = ?
            `).pluck();
        }
        return this._selectModifiedAtById.get(id);
    }

    /**
     * @param {string} id
     * @param {number} totalAdjustment
     * @param {number} handledAdjustment
     * @return {*}
     */
    adjustTotalAndHandledRequestCounts(id, totalAdjustment, handledAdjustment) {
        if (!this._adjustTotalAndHandledRequestCounts) {
            this._adjustTotalAndHandledRequestCounts = this.db.prepare(`
                UPDATE ${this.tableName}
                SET totalRequestCount = totalRequestCount + :totalAdjustment,
                    handledRequestCount = handledRequestCount + :handledAdjustment
                WHERE id = CAST(:id as INTEGER)
            `);
        }
        return this._adjustTotalAndHandledRequestCounts.run({
            id,
            totalAdjustment,
            handledAdjustment,
        });
    }

    /**
     * @param {string} id
     * @return {*}
     */
    updateAccessedAtById(id) {
        if (!this._updateQueueAccessedAtById) {
            this._updateQueueAccessedAtById = this.db.prepare(`
                UPDATE ${this.tableName}
                SET accessedAt = ${TIMESTAMP_SQL}
                WHERE id = CAST(? as INTEGER)
            `);
        }
        return this._updateQueueAccessedAtById.run(id);
    }

    /**
     * @param {RequestModel} requestModel
     * @return {number|null}
     */
    selectRequestOrderNoByModel(requestModel) {
        if (!this._selectRequestOrderNoByModel) {
            this._selectRequestOrderNoByModel = this.db.prepare(`
                SELECT orderNo FROM ${TABLE_NAMES.REQUEST_QUEUE_REQUESTS}
                WHERE queueId = CAST(:queueId as INTEGER) AND id = :id
            `).pluck();
        }
        return this._selectRequestOrderNoByModel.get(requestModel);
    }

    /**
     * @param {string} requestId
     * @param {string} queueId
     * @return {string}
     */
    selectRequestJsonByIdAndQueueId(requestId, queueId) {
        if (!this._selectRequestJsonByModel) {
            this._selectRequestJsonByModel = this.db.prepare(`
                SELECT json FROM ${TABLE_NAMES.REQUEST_QUEUE_REQUESTS}
                WHERE queueId = CAST(? as INTEGER) AND id = ?
            `).pluck();
        }
        return this._selectRequestJsonByModel.get(queueId, requestId);
    }

    /**
     * @param {string} queueId
     * @param {number} limit
     * @return {string[]}
     */
    selectRequestJsonsByQueueIdWithLimit(queueId, limit) {
        if (!this._selectRequestJsonsByQueueIdWithLimit) {
            this._selectRequestJsonsByQueueIdWithLimit = this.db.prepare(`
                SELECT json FROM ${TABLE_NAMES.REQUEST_QUEUE_REQUESTS}
                WHERE queueId = CAST(? as INTEGER) AND orderNo IS NOT NULL
                LIMIT ?
            `).pluck();
        }
        return this._selectRequestJsonsByQueueIdWithLimit.all(queueId, limit);
    }

    /**
     * @param {RequestModel} requestModel
     * @return {*}
     */
    insertRequestByModel(requestModel) {
        if (!this._insertRequestByModel) {
            this._insertRequestByModel = this.db.prepare(`
                INSERT INTO ${TABLE_NAMES.REQUEST_QUEUE_REQUESTS}(
                    id, queueId, orderNo, url, uniqueKey, method, retryCount, json
                ) VALUES (
                    :id, CAST(:queueId as INTEGER), :orderNo, :url, :uniqueKey, :method, :retryCount, :json
                )
            `);
        }
        return this._insertRequestByModel.run(requestModel);
    }

    /**
     * @param {RequestModel} requestModel
     * @return {*}
     */
    updateRequestByModel(requestModel) {
        if (!this._updateRequestByModel) {
            this._updateRequestByModel = this.db.prepare(`
                UPDATE ${TABLE_NAMES.REQUEST_QUEUE_REQUESTS}
                SET orderNo = :orderNo,
                    url = :url,
                    uniqueKey = :uniqueKey,
                    method = :method,
                    retryCount = :retryCount,
                    json = :json
                WHERE queueId = CAST(:queueId as INTEGER) AND id = :id
            `);
        }
        return this._updateRequestByModel.run(requestModel);
    }

    /**
     * @param {RequestModel} requestModel
     * @return {*}
     */
    addRequest(requestModel) {
        if (!this._addRequestTransaction) {
            this._addRequestTransaction = this.db.transaction((model) => {
                try {
                    this.insertRequestByModel(model);
                    const handledCountAdjustment = model.orderNo === null ? 1 : 0;
                    this.adjustTotalAndHandledRequestCounts(model.queueId, 1, handledCountAdjustment);
                    // We return wasAlreadyHandled: false even though the request may
                    // have been added as handled, because that's how API behaves.
                    return new QueueOperationInfo(model.id);
                } catch (err) {
                    if (err.code === ERROR_REQUEST_NOT_UNIQUE) {
                        // If we got here it means that the request was already present.
                        // We need to figure out if it were handled too.
                        const orderNo = this.selectRequestOrderNoByModel(model);
                        return new QueueOperationInfo(model.id, orderNo);
                    }
                    if (err.code === ERROR_QUEUE_DOES_NOT_EXIST) {
                        throw new Error(`Request queue with id: ${model.queueId} does not exist.`);
                    }
                    throw err;
                }
            });
        }
        return this._addRequestTransaction(requestModel);
    }

    /**
     * @param {RequestModel} requestModel
     * @return {*}
     */
    updateRequest(requestModel) {
        if (!this._updateRequestTransaction) {
            this._updateRequestTransaction = this.db.transaction((model) => {
                // First we need to check the existing request to be
                // able to return information about its handled state.
                const orderNo = this.selectRequestOrderNoByModel(model);

                // Undefined means that the request is not present in the queue.
                // We need to insert it, to behave the same as API.
                if (orderNo === undefined) {
                    return this.addRequest(model);
                }

                // When updating the request, we need to make sure that
                // the handled counts are updated correctly in all cases.
                this.updateRequestByModel(model);
                let handledCountAdjustment = 0;
                const isRequestHandledStateChanging = typeof orderNo !== typeof model.orderNo;
                const requestWasHandledBeforeUpdate = orderNo === null;

                if (isRequestHandledStateChanging) handledCountAdjustment += 1;
                if (requestWasHandledBeforeUpdate) handledCountAdjustment = -handledCountAdjustment;
                this.adjustTotalAndHandledRequestCounts(model.queueId, 0, handledCountAdjustment);

                // Again, it's important to return the state of the previous
                // request, not the new one, because that's how API does it.
                return new QueueOperationInfo(model.id, orderNo);
            });
        }
        return this._updateRequestTransaction(requestModel);
    }

    _createTables() {
        this.db.prepare(`
            CREATE TABLE IF NOT EXISTS ${this.tableName}(
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
            CREATE TABLE IF NOT EXISTS ${TABLE_NAMES.REQUEST_QUEUE_REQUESTS}(
                queueId INTEGER NOT NULL REFERENCES ${this.tableName}(id) ON DELETE CASCADE,
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
        const getSqlForCommand = (cmd) => `
        CREATE TRIGGER IF NOT EXISTS T_update_modifiedAt_on_${cmd.toLowerCase()}
                AFTER ${cmd} ON ${TABLE_NAMES.REQUEST_QUEUE_REQUESTS}
            BEGIN
                UPDATE ${this.tableName}
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
            ON ${TABLE_NAMES.REQUEST_QUEUE_REQUESTS}(queueId, orderNo)
            WHERE orderNo IS NOT NULL
        `).run();
    }
}

module.exports = RequestQueueDatabaseClient;
