exports.promisifyDbRun = (db) => {
    return async (...args) => {
        return new Promise((resolve, reject) => {
            db.run(...args, function (err) {
                if (err) return reject(err);
                resolve(this);
            });
        });
    };
};
