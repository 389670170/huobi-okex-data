/**
 * Created by haoran.shu on 2018/7/27 16:16.
 */
const MongoAdapt = require('./mongo-adapt');
const mongoConfig = require('../config').mongo;

const mongoAdapt = new MongoAdapt(mongoConfig);

module.exports = mongoAdapt;
