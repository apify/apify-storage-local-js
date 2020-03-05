const path = require('path');
const { promisify } = require('util');
const sqlite = require('sqlite3').verbose();

const APIFY_LOCAL_STORAGE_DIR = './apify_storage';

const filename = path.join(APIFY_LOCAL_STORAGE_DIR, 'local.db');

const data = [
    ['queue1', 'req1', 1, 'https://example.com/1', 0],
    ['queue1', 'req2', 2, 'https://example.com/1', 0],
    ['queue2', 'req1', 1, 'https://example.com/1', 0],
    ['queue1', 'req3', 3, 'https://example.com/1', 0],
    ['queue1', 'req4', -4, 'https://example.com/1', 0],
    ['queue1', 'req5', 5, 'https://example.com/1', 0],
    ['queue2', 'req2', 2, 'https://example.com/1', 0],
    ['queue1', 'req6', -6, 'https://example.com/1', 0],
];

const db = new sqlite.Database(filename, (err) => {
    if (err) console.error(err);
    else console.log('Connected');
});

// db.serialize(() => {
//     db.run(`
//     CREATE TABLE IF NOT EXISTS queues(
//         queueId TEXT NOT NULL,
//         requestId TEXT NOT NULL,
//         orderNo INTEGER NOT NULL,
//         requestUrl TEXT NOT NULL,
//         retryCount INTEGER NOT NULL,
//         PRIMARY KEY (queueId, requestId)
//     )`, (err) => {
//         if (err) console.error(err);
//     });
//     for (const params of data) {
//         db.run(`
//         INSERT INTO queues(queueId, requestId, orderNo, requestUrl, retryCount)
//         VALUES (?,?,?,?,?)
//     `, params, (err) => {
//             if (err) console.error(err);
//         });
//     }
// });


db.all(`
SELECT * FROM queues
WHERE queueId = ?
ORDER BY orderNo
`, ['queue1'], (err, row) => {
    if (err) console.error(err);
    else console.log(row);
});

db.close((err) => {
    if (err) console.error(err);
    else console.log('Disconnected');
});
