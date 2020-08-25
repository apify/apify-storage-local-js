const fs = require('fs-extra');
const ow = require('ow');
const path = require('path');

/**
 * This is what API returns in the x-apify-pagination-limit
 * header when no limit query parameter is used.
 * @type {number}
 */
const LIST_ITEMS_LIMIT = 999999999999;

/**
 * Number of characters of the dataset item file names.
 * E.g.: 000000019.json - 9 digits
 * @type {number}
 */
const LOCAL_FILENAME_DIGITS = 9;

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
        this.itemCount = undefined;
    }

    async export() {
        throw new Error('This method is not implemented in @apify/storage-local yet.');
    }

    async listItems(options = {}) {
        this._ensureItemCount();
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
            desc,
        } = options;

        const [start, end] = this._getStartAndEndIndexes(offset, limit);
        const items = [];
        for (let idx = start; idx < end; idx++) {
            const item = await this._readAndParseFile(idx);
            items.push(item);
        }

        return {
            items: desc ? items.reverse() : items,
            total: this.itemCount,
            offset,
            count: items.length,
            limit,
        };
    }

    /**
     * @param {Object|string|Object[]|string[]}items
     * @return {Promise<void>}
     */
    async pushItems(items) {
        this._ensureItemCount();
        ow(items, ow.any(
            ow.object,
            ow.string,
            ow.array.ofType(ow.any(ow.object, ow.string)),
        ));

        if (!Array.isArray(items)) items = [items];
        const promises = items.map((item) => {
            this.counter++;

            if (typeof item !== 'string') item = JSON.stringify(item, null, 2);
            const filePath = path.join(this.storeDir, this._getItemFileName(this.counter));

            return fs.writeFile(filePath, item);
        });
        await Promise.all(promises);
    }

    _ensureItemCount() {
        if (typeof this.itemCount === 'number') return;

        let files;
        try {
            files = fs.readdirSync(this.storeDir);
        } catch (err) {
            if (err.code === 'ENOENT') {
                throw new Error(`Dataset with id: ${this.name} does not exist.`);
            } else {
                throw err;
            }
        }

        if (files.length) {
            const lastFile = files.pop();
            const lastFileName = path.parse(lastFile).name;
            this.itemCount = Number(lastFileName);
        } else {
            this.itemCount = 0;
        }
    }

    _getItemFileName(index) {
        return `${index}.json`.padStart(LOCAL_FILENAME_DIGITS, '0');
    }

    /**
     * @param {number} offset
     * @param {number} limit
     * @return {[number, number]}
     * @private
     */
    _getStartAndEndIndexes(offset, limit = this.itemCount) {
        const start = offset + 1;
        const end = Math.min(offset + limit, this.itemCount) + 1;
        return [start, end];
    }

    /**
     * @param {number} index
     * @return {Promise<Object>}
     * @private
     */
    async _readAndParseFile(index) {
        const filePath = path.join(this.storeDir, this._getItemFileName(index));

        const json = await fs.readFile(filePath, 'utf8');
        return JSON.parse(json);
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
