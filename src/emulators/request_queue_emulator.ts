import { join, parse } from 'path';
import type { Database, Statement, Transaction, RunResult } from 'better-sqlite3';
import { QueueOperationInfo } from './queue_operation_info';
import { STORAGE_NAMES, TIMESTAMP_SQL, DATABASE_FILE_NAME } from '../consts';
import type { DatabaseConnectionCache } from '../database_connection_cache';
import type { ProlongRequestLockOptions, RequestModel, RequestOptions } from '../resource_clients/request_queue';
import { ProcessedRequest } from './batch_add_requests/processed_request';
import { UnprocessedRequest } from './batch_add_requests/unprocessed_request';

const ERROR_REQUEST_NOT_UNIQUE = 'SQLITE_CONSTRAINT_PRIMARYKEY';
const ERROR_QUEUE_DOES_NOT_EXIST = 'SQLITE_CONSTRAINT_FOREIGNKEY';

export interface RequestQueueEmulatorOptions {
    queueDir: string;
    dbConnections: DatabaseConnectionCache
}

interface ErrorWithCode extends Error {
    code: string;
}

export interface RawQueueTableData {
    id: string;
    name: string;
    createdAt: string;
    modifiedAt: string;
    accessedAt: string;
    totalRequestCount: number;
    handledRequestCount: number;
    pendingRequestCount: number;
}

export interface RequestQueueInfo {
    id: string;
    name: string;
    createdAt: Date;
    modifiedAt: Date;
    accessedAt: Date;
    totalRequestCount: number;
    handledRequestCount: number;
    pendingRequestCount: number;
}

export interface RawRequestsTableData {
    queueId: string;
    id: string;
    orderNo: number;
    url: string;
    uniqueKey: string;
    method?: string | null;
    retryCount: number;
    json: string;
}

interface RequestQueueStatements {
    selectById: Statement<[id: string | number]>;
    deleteById: Statement<[id: string]>;
    selectByName: Statement<[name: string]>;
    selectModifiedAtById: Statement<[id: string | number]>;
    insertByName: Statement<[name: string]>;
    updateNameById: Statement<[{ id: string | number; name: string }]>;
    updateModifiedAtById: Statement<[id: string | number]>;
    updateAccessedAtById: Statement<[id: string | number]>;
    adjustTotalAndHandledRequestCounts: Statement<[{ id: string; totalAdjustment: number; handledAdjustment: number }]>;
    selectRequestOrderNoByModel: Statement<[requestModel: RequestModel]>;
    selectRequestJsonByModel: Statement<[{ requestId: string; queueId: string }]>;
    selectRequestJsonsByQueueIdWithLimit: Statement<[{queueId: string, limit: number}]>;
    insertRequestByModel: Statement<[requestModel: RequestModel]>;
    updateRequestByModel: Statement<[requestModel: RequestModel]>;
    deleteRequestById: Statement<[id: string]>;
    fetchRequestNotExpired: Statement<[id: string]>;
    fetchRequestNotExpiredAndLocked: Statement<{ id: string; currentTime: number }>;
    updateOrderNo: Statement<{ id: string; orderNo: number }>;
    fetchRequestHeadThatWillBeLocked: Statement<{ queueId: string; limit: number; currentTime: number; }>;
}

interface RequestQueueTransactions {
    addRequest: Transaction<(requestModel: RequestModel) => QueueOperationInfo>;
    selectOrInsertByName: Transaction<(name: string) => RawQueueTableData>;
    batchAddRequests: Transaction<(requestModels: RequestModel[]) => BatchAddRequestsResult>;
    updateRequest: Transaction<(requestModel: RequestModel) => QueueOperationInfo>;
    deleteRequest: Transaction<(id: string) => unknown>;
    prolongRequestLock: Transaction<(id: string, options: ProlongRequestLockOptions) => Date>;
    deleteRequestLock: Transaction<(id: string, options: RequestOptions) => void>;
    listAndLockHead: Transaction<(queueId: string, limit: number, lockSecs: number) => string[]>;
}

export class RequestQueueEmulator {
    dbPath: string;

    dbConnections: DatabaseConnectionCache;

    db: Database;

    queueTableName = STORAGE_NAMES.REQUEST_QUEUES;

    requestsTableName = `${STORAGE_NAMES.REQUEST_QUEUES}_requests`;

    private statements: RequestQueueStatements = null!;
    private transactions: RequestQueueTransactions = null!;

    constructor({ queueDir, dbConnections }: RequestQueueEmulatorOptions) {
        this.dbPath = join(queueDir, DATABASE_FILE_NAME);
        this.dbConnections = dbConnections;

        try {
            this.db = dbConnections.openConnection(this.dbPath);
        } catch (err) {
            if (err.code !== 'ENOENT') throw err;
            const newError = new Error(`Request queue with id: ${parse(queueDir).name} does not exist.`) as ErrorWithCode;
            newError.code = 'ENOENT';
            throw newError;
        }

        // Everything's covered by IF NOT EXISTS so no need
        // to worry that multiple entities will be created.
        this._createTables();
        this._createTriggers();
        this._createIndexes();
        this._createStatements();
        this._createTransactions();
    }

    /**
     * Disconnects the emulator from the underlying database.
     */
    disconnect(): void {
        this.dbConnections.closeConnection(this.dbPath);
    }

    selectById(id: string | number): RawQueueTableData {
        return this.statements.selectById.get(id) as RawQueueTableData;
    }

    deleteById(id: string): RunResult {
        return this.statements.deleteById.run(id);
    }

    selectByName(name: string): RawQueueTableData {
        return this.statements.selectByName.get(name) as RawQueueTableData;
    }

    insertByName(name: string): RunResult {
        return this.statements.insertByName.run(name);
    }

    selectOrInsertByName(name: string): RawQueueTableData {
        return this.transactions.selectOrInsertByName(name) as RawQueueTableData;
    }

    selectModifiedAtById(id: string | number): string {
        return this.statements.selectModifiedAtById.get(id) as string;
    }

    updateNameById(id: string | number, name: string): RunResult {
        return this.statements.updateNameById.run({ id, name });
    }

    updateModifiedAtById(id: string | number): RunResult {
        return this.statements.updateModifiedAtById.run(id);
    }

    updateAccessedAtById(id: string | number): RunResult {
        return this.statements.updateAccessedAtById.run(id);
    }

    adjustTotalAndHandledRequestCounts(id: string, totalAdjustment: number, handledAdjustment: number): RunResult {
        return this.statements.adjustTotalAndHandledRequestCounts.run({
            id,
            totalAdjustment,
            handledAdjustment,
        });
    }

    selectRequestOrderNoByModel(requestModel: RequestModel): number | null {
        return this.statements.selectRequestOrderNoByModel.get(requestModel) as number;
    }

    selectRequestJsonByIdAndQueueId(requestId: string, queueId: string): string {
        return this.statements.selectRequestJsonByModel.get({ queueId, requestId }) as string;
    }

    selectRequestJsonsByQueueIdWithLimit(queueId: string, limit: number): string[] {
        return this.statements.selectRequestJsonsByQueueIdWithLimit.all({ queueId, limit }) as string[];
    }

    insertRequestByModel(requestModel: RequestModel): RunResult {
        return this.statements.insertRequestByModel.run(requestModel);
    }

    updateRequestByModel(requestModel: RequestModel): RunResult {
        return this.statements.updateRequestByModel.run(requestModel);
    }

    deleteRequestById(id: string): RunResult {
        return this.statements.deleteById.run(id);
    }

    addRequest(requestModel: RequestModel): QueueOperationInfo {
        return this.transactions.addRequest(requestModel) as QueueOperationInfo;
    }

    batchAddRequests(requestModels: RequestModel[]): BatchAddRequestsResult {
        return this.transactions.batchAddRequests(requestModels);
    }

    updateRequest(requestModel: RequestModel): QueueOperationInfo {
        return this.transactions.updateRequest(requestModel);
    }

    deleteRequest(id: string): unknown {
        return this.transactions.deleteRequest(id);
    }

    prolongRequestLock(id: string, options: ProlongRequestLockOptions) {
        return this.transactions.prolongRequestLock(id, options);
    }

    deleteRequestLock(id: string, options: RequestOptions) {
        return this.transactions.deleteRequestLock(id, options);
    }

    listAndLockHead(queueId: string, limit: number, lockSecs: number): string[] {
        return this.transactions.listAndLockHead(queueId, limit, lockSecs);
    }

    private updateOrderNo({ id, orderNo }: { id: string; orderNo: number; }) {
        this.statements.updateOrderNo.run({ id, orderNo });
    }

    private _createTables() {
        this.db.prepare(`
            CREATE TABLE IF NOT EXISTS ${this.queueTableName}(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                createdAt TEXT DEFAULT(${TIMESTAMP_SQL}),
                modifiedAt TEXT DEFAULT(${TIMESTAMP_SQL}),
                accessedAt TEXT DEFAULT(${TIMESTAMP_SQL}),
                totalRequestCount INTEGER DEFAULT 0,
                handledRequestCount INTEGER DEFAULT 0,
                pendingRequestCount INTEGER GENERATED ALWAYS AS (totalRequestCount - handledRequestCount) VIRTUAL
            )
        `).run();
        this.db.prepare(`
            CREATE TABLE IF NOT EXISTS ${this.requestsTableName}(
                queueId INTEGER NOT NULL REFERENCES ${this.queueTableName}(id) ON DELETE CASCADE,
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

    private _createTriggers() {
        const getSqlForRequests = (cmd: 'INSERT' | 'UPDATE' | 'DELETE') => `
        CREATE TRIGGER IF NOT EXISTS T_bump_modifiedAt_accessedAt_on_${cmd.toLowerCase()}
                AFTER ${cmd} ON ${this.requestsTableName}
            BEGIN
                UPDATE ${this.queueTableName}
                SET modifiedAt = ${TIMESTAMP_SQL},
                    accessedAt = ${TIMESTAMP_SQL}
                WHERE id = ${cmd === 'DELETE' ? 'OLD' : 'NEW'}.queueId;
            END
        `;

        (['INSERT', 'UPDATE', 'DELETE'] as const).forEach((cmd) => {
            const sql = getSqlForRequests(cmd);
            this.db.exec(sql);
        });
    }

    private _createIndexes() {
        this.db.prepare(`
            CREATE INDEX IF NOT EXISTS I_queueId_orderNo
            ON ${this.requestsTableName}(queueId, orderNo)
            WHERE orderNo IS NOT NULL
        `).run();
    }

    private _createStatements() {
        this.statements = {
            selectById: this.db.prepare(/* sql */`
                SELECT *, CAST(id as TEXT) as id
                FROM ${this.queueTableName}
                WHERE id = ?
            `),
            deleteById: this.db.prepare(/* sql */`
                DELETE FROM ${this.queueTableName}
                WHERE id = CAST(? as INTEGER)
            `),
            selectByName: this.db.prepare(/* sql */`
                SELECT *, CAST(id as TEXT) as id
                FROM ${this.queueTableName}
                WHERE name = ?
            `),
            insertByName: this.db.prepare(/* sql */`
                INSERT INTO ${this.queueTableName}(name)
                VALUES(?)
            `),
            selectModifiedAtById: this.db.prepare(/* sql */`
                SELECT modifiedAt
                FROM ${this.queueTableName}
                WHERE id = ?
            `).pluck(),
            updateNameById: this.db.prepare(/* sql */`
                UPDATE ${this.queueTableName}
                SET name = :name
                WHERE id = CAST(:id as INTEGER)
            `),
            updateModifiedAtById: this.db.prepare(/* sql */`
                UPDATE ${this.queueTableName}
                SET modifiedAt = ${TIMESTAMP_SQL}
                WHERE id = CAST(? as INTEGER)
            `),
            updateAccessedAtById: this.db.prepare(/* sql */`
                UPDATE ${this.queueTableName}
                SET accessedAt = ${TIMESTAMP_SQL}
                WHERE id = CAST(? as INTEGER)
            `),
            adjustTotalAndHandledRequestCounts: this.db.prepare(/* sql */`
                UPDATE ${this.queueTableName}
                SET totalRequestCount = totalRequestCount + :totalAdjustment,
                    handledRequestCount = handledRequestCount + :handledAdjustment
                WHERE id = CAST(:id as INTEGER)
            `),
            selectRequestOrderNoByModel: this.db.prepare(/* sql */`
                SELECT orderNo FROM ${this.requestsTableName}
                WHERE queueId = CAST(:queueId as INTEGER) AND id = :id
            `).pluck(),
            selectRequestJsonByModel: this.db.prepare(/* sql */`
                SELECT "json" FROM ${this.requestsTableName}
                WHERE queueId = CAST(:queueId as INTEGER) AND id = :id
            `).pluck(),
            selectRequestJsonsByQueueIdWithLimit: this.db.prepare(/* sql */`
                SELECT "json" FROM ${this.requestsTableName}
                WHERE queueId = CAST(:queueId as INTEGER) AND orderNo IS NOT NULL
                LIMIT :limit
            `).pluck(),
            insertRequestByModel: this.db.prepare(/* sql */`
                INSERT INTO ${this.requestsTableName}(
                    id, queueId, orderNo, url, uniqueKey, method, retryCount, json
                ) VALUES (
                    :id, CAST(:queueId as INTEGER), :orderNo, :url, :uniqueKey, :method, :retryCount, :json
                )
            `),
            updateRequestByModel: this.db.prepare(/* sql */`
                UPDATE ${this.requestsTableName}
                SET orderNo = :orderNo,
                    url = :url,
                    uniqueKey = :uniqueKey,
                    method = :method,
                    retryCount = :retryCount,
                    json = :json
                WHERE queueId = CAST(:queueId as INTEGER) AND id = :id
            `),
            deleteRequestById: this.db.prepare(/* sql */`
                DELETE FROM ${this.requestsTableName}
                WHERE id = ?
            `),
            fetchRequestNotExpired: this.db.prepare(/* sql */`
                SELECT id, orderNo FROM ${this.requestsTableName}
                WHERE id = ?
                AND orderNo IS NOT NULL
            `),
            fetchRequestNotExpiredAndLocked: this.db.prepare(/* sql */`
                SELECT id FROM ${this.requestsTableName}
                WHERE id = :id
                AND orderNo IS NOT NULL
                AND (
                    orderNo > :currentTime
                    OR orderNo < -(:currentTime)
                )
            `),
            fetchRequestHeadThatWillBeLocked: this.db.prepare(/* sql */`
                SELECT id, "json", orderNo FROM ${this.requestsTableName}
                WHERE queueId = CAST(:queueId as INTEGER)
                AND orderNo IS NOT NULL
                AND orderNo <= :currentTime
                AND orderNo >= -(:currentTime)
                ORDER BY orderNo ASC
                LIMIT :limit
            `),
            updateOrderNo: this.db.prepare(/* sql */`
                UPDATE ${this.requestsTableName}
                SET orderNo = :orderNo
                WHERE id = :id
            `),
        };
    }

    private _createTransactions() {
        this.transactions = {
            selectOrInsertByName: this.db.transaction((name) => {
                if (name) {
                    const storage = this.selectByName(name);
                    if (storage) return storage;
                }

                const { lastInsertRowid } = this.insertByName(name);
                return this.selectById(lastInsertRowid.toString());
            }),
            addRequest: this.db.transaction((model) => {
                try {
                    this.insertRequestByModel(model);
                    const handledCountAdjustment = model.orderNo === null ? 1 : 0;
                    this.adjustTotalAndHandledRequestCounts(model.queueId!, 1, handledCountAdjustment);
                    // We return wasAlreadyHandled: false even though the request may
                    // have been added as handled, because that's how API behaves.
                    return new QueueOperationInfo(model.id!);
                } catch (err) {
                    if (err.code === ERROR_REQUEST_NOT_UNIQUE) {
                        // If we got here it means that the request was already present.
                        // We need to figure out if it were handled too.
                        const orderNo = this.selectRequestOrderNoByModel(model);
                        return new QueueOperationInfo(model.id!, orderNo);
                    }
                    if (err.code === ERROR_QUEUE_DOES_NOT_EXIST) {
                        throw new Error(`Request queue with id: ${model.queueId} does not exist.`);
                    }
                    throw err;
                }
            }),
            batchAddRequests: this.db.transaction((models) => {
                const result: BatchAddRequestsResult = {
                    processedRequests: [],
                    unprocessedRequests: [],
                };

                for (const model of models) {
                    try {
                        this.insertRequestByModel(model);
                        const handledCountAdjustment = model.orderNo == null ? 1 : 0;
                        this.adjustTotalAndHandledRequestCounts(model.queueId!, 1, handledCountAdjustment);
                        // We return wasAlreadyHandled: false even though the request may
                        // have been added as handled, because that's how API behaves.
                        result.processedRequests.push(new ProcessedRequest(model.id!, model.uniqueKey));
                    } catch (err) {
                        if (err.code === ERROR_REQUEST_NOT_UNIQUE) {
                            const orderNo = this.selectRequestOrderNoByModel(model);
                            // If we got here it means that the request was already present.
                            result.processedRequests.push(new ProcessedRequest(model.id!, model.uniqueKey, orderNo));
                        } else if (err.code === ERROR_QUEUE_DOES_NOT_EXIST) {
                            throw new Error(`Request queue with id: ${model.queueId} does not exist.`);
                        } else {
                            throw err;
                        }
                    }
                }

                return result;
            }),
            updateRequest: this.db.transaction((model) => {
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
                this.adjustTotalAndHandledRequestCounts(model.queueId!, 0, handledCountAdjustment);

                // Again, it's important to return the state of the previous
                // request, not the new one, because that's how API does it.
                return new QueueOperationInfo(model.id!, orderNo);
            }),
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            deleteRequest: this.db.transaction((_id) => {
                // TODO
            }),
            prolongRequestLock: this.db.transaction((id, options) => {
                const existingRequest = this.statements.fetchRequestNotExpired.get(id) as { orderNo: number; id: string } | undefined;

                if (!existingRequest) {
                    throw new Error(`Request with ID ${id} was already handled or doesn't exist`);
                }

                const unlockTimestamp = Math.abs(existingRequest.orderNo) + options.lockSecs * 1000;
                const newOrderNo = options.forefront ? -unlockTimestamp : unlockTimestamp;

                this.updateOrderNo({ id, orderNo: newOrderNo });

                return new Date(unlockTimestamp);
            }),
            deleteRequestLock: this.db.transaction((id, { forefront }) => {
                const timestamp = Date.now();

                const existingRequest = this.statements.fetchRequestNotExpiredAndLocked.get({
                    id,
                    currentTime: timestamp,
                }) as { id: string } | undefined;

                if (!existingRequest) {
                    throw new Error(`Request with ID ${id} was already handled, doesn't exist, or is not locked`);
                }

                this.updateOrderNo({ id, orderNo: forefront ? -timestamp : timestamp });
            }),
            listAndLockHead: this.db.transaction((queueId, limit, lockSecs) => {
                const timestamp = Date.now();

                const requestsToLock = this.statements.fetchRequestHeadThatWillBeLocked.all({
                    queueId,
                    currentTime: timestamp,
                    limit,
                }) as { id: string; json: string; orderNo: number }[];

                if (!requestsToLock.length) {
                    return [];
                }

                for (const { id, orderNo } of requestsToLock) {
                    const newOrderNo = (timestamp + lockSecs * 1000) * (orderNo > 0 ? 1 : -1);

                    this.updateOrderNo({ id, orderNo: newOrderNo });
                }

                return requestsToLock.map(({ json }) => json);
            }),
        };
    }
}

export interface BatchAddRequestsResult {
    processedRequests: ProcessedRequest[];
    unprocessedRequests: UnprocessedRequest[];
}
