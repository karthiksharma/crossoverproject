"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const redis = require("redis");
const util = require("util");
const KEY = `account1/balance`;
const DEFAULT_BALANCE = 100;

exports.chargeRequestRedis = async function (input) {
    var charges = getCharges();
    const redisClient = await getRedisClient();
    try {
        // Start a Redis transaction
        const multi = redisClient.multi();

        // Watch the balance key for changes
        multi.watch(KEY);

        // Get the remaining balance
        const remainingBalance = await getBalanceRedis(redisClient, KEY);

        // Check if the request is authorized
        const isAuthorized = authorizeRequest(remainingBalance, charges);
        if (!isAuthorized) {
            multi.discard(KEY);
            return {
                remainingBalance,
                isAuthorized,
                charges: 0,
            };
        }

        // Perform the charge operation
        multi.decrby(KEY, charges);

        // Execute the transaction
        const results = await util.promisify(multi.exec).call(multi);

        if (!results) {
            // Transaction failed, retry or handle the failure
            throw new Error("Transaction failed. Retry or handle the failure.");
        }

        // Get the updated balance
        const updatedBalance = await getBalanceRedis(redisClient, KEY);

        return {
            remainingBalance: updatedBalance,
            charges,
            isAuthorized,
        };
    } catch (error) {
        // Handle transaction failure or other errors
        console.error("An error occurred:", error);
        throw error;
    }
};
exports.resetRedis = async function () {
    const redisClient = await getRedisClient();
    const ret = new Promise((resolve, reject) => {
        redisClient.set(KEY, String(DEFAULT_BALANCE), (err, res) => {
            if (err) {
                reject(err);
            }
            else {
                resolve(DEFAULT_BALANCE);
            }
        });
    });
    await disconnectRedis(redisClient);
    return ret;
};
async function getRedisClient() {
    return new Promise((resolve, reject) => {
        try {
            const client = new redis.RedisClient({
                host: process.env.ENDPOINT,
                port: parseInt(process.env.PORT || "6379"),
            });
            client.on("ready", () => {
                console.log('redis client ready');
                resolve(client);
            });
        }
        catch (error) {
            reject(error);
        }
    });
}
async function disconnectRedis(client) {
    return new Promise((resolve, reject) => {
        client.quit((error, res) => {
            if (error) {
                reject(error);
            }
            else if (res == "OK") {
                console.log('redis client disconnected');
                resolve(res);
            }
            else {
                reject("unknown error closing redis connection.");
            }
        });
    });
}
function authorizeRequest(remainingBalance, charges) {
    return remainingBalance >= charges;
}
function getCharges() {
    return DEFAULT_BALANCE / 20;
}
async function getBalanceRedis(redisClient, key) {
    const res = await util.promisify(redisClient.get).bind(redisClient).call(redisClient, key);
    return parseInt(res || "0");
}
async function chargeRedis(redisClient, key, charges) {
    return util.promisify(redisClient.decrby).bind(redisClient).call(redisClient, key, charges);
}
