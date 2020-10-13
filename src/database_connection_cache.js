const Database = require('better-sqlite3-with-prebuilds');

/**
 * SQLite prefers to have a single connection shared by
 * all users instead of opening and closing multiple connections.
 */
class DatabaseConnectionCache {
    constructor() {
        /** @type {Map<string,Database>} */
        this.connections = new Map();
    }

    /**
     * @param {string} path
     * @param {object} [options]
     * @return {Database}
     */
    openConnection(path, options) {
        const existingConnection = this.connections.get(path);
        if (existingConnection) return existingConnection;

        const newConnection = this._createConnection(path, options);
        this.connections.set(path, newConnection);
        return newConnection;
    }

    /**
     * Closes database connection and keeps the data. Should
     * be called at the end of use to allow the process to exit
     * gracefully. No further database operations will be executed.
     *
     * @param {string} path
     */
    closeConnection(path) {
        const connection = this.connections.get(path);
        if (connection) connection.close();
    }

    closeAllConnections() {
        this.connections.forEach((conn) => conn.close());
    }

    /**
     * Closes the database connection and removes all data.
     * With file system databases, it deletes the database file.
     * No further database operations will be executed.
     */
    // dropDatabase() {
    //     this.db.close();
    //     if (this.inMemory) return;
    //     fs.unlinkSync(this.dbFilePath);
    //
    //     // It seems that the extra 2 files are automatically deleted
    //     // when the original file is deleted, but I'm not sure if
    //     // this applies to all OSs.
    //     DATABASE_FILE_SUFFIXES.forEach((suffix) => {
    //         try {
    //             fs.unlinkSync(`${this.dbFilePath}${suffix}`);
    //         } catch (err) {
    //             if (err.code !== 'ENOENT') throw err;
    //         }
    //     });
    // }

    /**
     * @param {string} path
     * @param {object} options
     * @return {Database}
     * @private
     */
    _createConnection(path, options) {
        let connection;
        try {
            connection = new Database(path, options);
        } catch (err) {
            if (/cannot open database because the directory does not exist/i.test(err.message)) {
                err.code = 'ENOENT';
                throw err;
            }
            throw new Error(`Connection to database could not be established at ${path}\nCause: ${err.message}`);
        }
        // WAL mode should greatly improve performance
        // https://github.com/JoshuaWise/better-sqlite3/blob/master/docs/performance.md
        connection.exec('PRAGMA journal_mode = WAL');
        connection.exec('PRAGMA foreign_keys = ON');
        return connection;
    }
}

module.exports = DatabaseConnectionCache;
