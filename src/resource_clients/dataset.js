const fs = require('fs-extra');
const ow = require('ow');
const path = require('path');

const LIST_ITEMS_LIMIT = 250000;

class DatasetClient {
    /**
     * @param {object} options
     * @param {string} options.id
     * @param {string} options.storageDir
     */
    constructor(options) {
        const {
            name,
            storageDir,
        } = options;

        this.name = name;
        this.storeDir = path.join(storageDir, name);
    }

    async export() {
        throw new Error('This method is not implemented in @apify/storage-local yet.');
    }

    async listItems(options = {}) {
        // The extra code is to enable a custom validation message.
        ow(options, ow.object.validate((value) => ({
            validator: ow.isValid(value, ow.object.exactShape({
                // clean: ow.optional.boolean,
                desc: ow.optional.boolean,
                // fields: ow.optional.array.ofType(ow.string),
                // omit: ow.optional.array.ofType(ow.string),
                limit: ow.optional.number,
                offset: ow.optional.number,
                // skipEmpty: ow.optional.boolean,
                // skipHidden: ow.optional.boolean,
                // unwind: ow.optional.string,
            })),
            message: 'Local dataset emulation supports only the "desc", "limit" and "offset" options.',
        })));

        const {
            limit = LIST_ITEMS_LIMIT,
            offset = 0,
        } = options;

        const indexes = this._getItemIndexes(offset, limit);
        const items = [];
        for (const idx of indexes) {
            const item = await this._readAndParseFile(idx);
            items.push(item);
        }

        return {
            items: opts.desc ? items.reverse() : items,
            total: this.counter,
            offset: opts.offset,
            count: items.length,
            limit: opts.limit,
        };
    }

    async pushItems(items) {
        ow(items, ow.any(ow.object, ow.array, ow.string));

        await this.httpClient.call({
            url: this._url('items'),
            method: 'POST',
            data: items,
            params: this._params(),
        });
    }

    /**
     * Returns an array of item indexes for given offset and limit.
     */
    _getItemIndexes(offset = 0, limit = this.counter) {
        if (limit === null) throw new Error('DatasetLocal must be initialized before calling this._getItemIndexes()!');
        const start = offset + 1;
        const end = Math.min(offset + limit, this.counter) + 1;
        if (start > end) return [];
        return _.range(start, end);
    }

    /**
     * Reads and parses file for given index.
     */
    _readAndParseFile(index) {
        const filePath = path.join(this.localStoragePath, getLocaleFilename(index));

        return readFilePromised(filePath)
            .then((json) => {
                this._updateMetadata();
                return JSON.parse(json);
            });
    }
}

module.exports = DatasetClient;

/**
 * @typedef {object} PaginationList
 * @property {object[]} items - List of returned objects
 * @property {number} total - Total number of objects
 * @property {number} offset - Number of objects that were skipped
 * @property {number} count - Number of returned objects
 * @property {number} [limit] - Requested limit
 */
