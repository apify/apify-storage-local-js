class DatabaseClient {
    constructor(database, tableName) {
        this.db = database;
        this.tableName = tableName;
    }

    /**
     * @param {string} sql
     * @return {*}
     */
    runSql(sql) {
        const statement = this.db.prepare(sql);
        if (/^\s*SELECT/.test(sql)) {
            return statement.get();
        }
        return statement.run();
    }

    /**
     * @param {string} id
     * @return {*}
     */
    selectById(id) {
        if (!this._selectById) {
            this._selectById = this.db.prepare(`
                SELECT *, CAST(id as TEXT) as id
                FROM ${this.tableName}
                WHERE id = ?
            `);
        }
        return this._selectById.get(id);
    }

    /**
     * @param {string} id
     * @return {*}
     */
    deleteById(id) {
        if (!this._deleteById) {
            this._deleteById = this.db.prepare(`
                DELETE FROM ${this.tableName}
                WHERE id = CAST(? as INTEGER)
            `);
        }
        return this._deleteById.run(id);
    }

    /**
     * @param {string} name
     * @return {*}
     */
    selectByName(name) {
        if (!this._selectByName) {
            this._selectByName = this.db.prepare(`
                SELECT *, CAST(id as TEXT) as id
                FROM ${this.tableName}
                WHERE name = ?
            `);
        }
        return this._selectByName.get(name);
    }

    /**
     * @param {string} name
     * @return {*}
     */
    insertByName(name) {
        if (!this._insertByName) {
            this._insertByName = this.db.prepare(`
                INSERT INTO ${this.tableName}(name)
                VALUES(?)
            `);
        }
        return this._insertByName.run(name);
    }

    selectOrInsertByName(name) {
        if (!this._selectOrInsertTransaction) {
            this._selectOrInsertTransaction = this.db.transaction((n) => {
                if (n) {
                    const storage = this.selectByName(n);
                    if (storage) return storage;
                }

                const { lastInsertRowid } = this.insertByName(n);
                return this.selectById(lastInsertRowid);
            });
        }
        return this._selectOrInsertTransaction(name);
    }
}

module.exports = DatabaseClient;
