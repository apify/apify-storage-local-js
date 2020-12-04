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
        this.updatingTimestamps = false;
    }

    /**
     * @return {Promise<Dataset>}
     */
    async get() {
        try {
            this._ensureItemCount();
            const stats = await fs.stat(this.storeDir);
            // The platform treats writes as access, but filesystem does not,
            // so if the modification time is more recent, use that.
            const accessedTimestamp = Math.max(stats.mtime.getTime(), stats.atime.getTime());
            return {
                id: this.name,
                name: this.name,
                createdAt: stats.birthtime,
                modifiedAt: stats.mtime,
                accessedAt: new Date(accessedTimestamp),
                itemCount: this.itemCount,
            };
        } catch (err) {
            if (err.code !== 'ENOENT') throw err;
        }
    }

    /**
     * @param {object} newFields
     * @param {string} [newFields.name]
     * @return {Promise<void>}
     */
    async update(newFields) {
        // The validation is intentionally loose to prevent issues
        // when swapping to a remote storage in production.
        ow(newFields, ow.object.partialShape({
            name: ow.optional.string.minLength(1),
        }));
        if (!newFields.name) return;

        const newPath = path.join(path.dirname(this.storeDir), newFields.name);
        try {
            await fs.move(this.storeDir, newPath);
        } catch (err) {
            if (/dest already exists/.test(err.message)) {
                throw new Error('Dataset name is not unique.');
            } else if (err.code === 'ENOENT') {
                this._throw404();
            } else {
                throw err;
            }
        }
        this.name = newFields.name;
    }

    async delete() {
        await fs.remove(this.storeDir);
    }

    async export() {
        throw new Error('This method is not implemented in @apify/storage-local yet.');
    }

    /**
     * @param {object} [options]
     * @param {boolean} [options.desc]
     * @param {number} [options.limit]
     * @param {number} [options.offset]
     * @return {Promise<PaginationList>}
     */
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

        this._updateTimestamps();
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
            this.itemCount++;

            if (typeof item !== 'string') item = JSON.stringify(item, null, 2);
            const filePath = path.join(this.storeDir, this._getItemFileName(this.itemCount));

            return fs.writeFile(filePath, item);
        });

        await Promise.all(promises);
        this._updateTimestamps({ mtime: true });
    }

    /**
     * @private
     */
    _ensureItemCount() {
        if (typeof this.itemCount === 'number') return;

        let files;
        try {
            files = fs.readdirSync(this.storeDir);
        } catch (err) {
            if (err.code === 'ENOENT') {
                this._throw404();
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

    /**
     * @param {number} index
     * @return {string}
     * @private
     */
    _getItemFileName(index) {
        const name = `${index}`.padStart(LOCAL_FILENAME_DIGITS, '0');
        return `${name}.json`;
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

    /**
     * @private
     */
    _throw404() {
        const err = new Error(`Dataset with id: ${this.name} does not exist.`);
        err.code = 'ENOENT';
        throw err;
    }

    /**
     * @param {object} [options]
     * @param {boolean} [options.mtime]
     * @private
     */
    _updateTimestamps({ mtime } = {}) {
        // It's throwing EINVAL on Windows. Not sure why,
        // so the function is a best effort only.
        const now = new Date();
        let promise;
        if (mtime) {
            promise = fs.utimes(this.storeDir, now, now);
        } else {
            promise = fs.stat(this.storeDir)
                .then((stats) => fs.utimes(this.storeDir, now, stats.mtime));
        }
        promise.catch(() => { /* we don't care that much if it sometimes fails */ });
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
