/**!
 * mongo-adapt - index.js
 * Copyright(c) 2018 Tenny
 * MIT Licensed
 */
const { MongoClient, ObjectID } = require('mongodb');

class MongoAdapt {

  constructor(mongos) {
    this.conn(mongos); // 建立数据库连接
  }

  /**
   * 连接到mongo数据库
   * @param mongos {Object} {
   * 配置的第一个连接将作为默认的连接, 后续的所有的数据库的操作如果没有传递连接名称，将使用默认的连接
   *   name1: '',
   *   name2: '',
   *   ……
   * }
   */
  conn(mongos) {
    if (mongos && mongos instanceof Object) {
      // 默认使用的连接名, 使用的时候, 如果没有传使用某个连接, 则使用这个默认的
      this.mconn = null;
      // 数据库连接成功后, 缓存所有的连接, 避免每次访问数据库的时候，都进行连接
      this.clients = new Map();
      for (let key in mongos) {
        if (Object.prototype.hasOwnProperty.call(mongos, key)) {
          MongoClient.connect(mongos[key], { useNewUrlParser: true }).then((client) => {
            if (!this.mconn) {
              this.mconn = key; // 缓存第一个连接, 作为以后的默认使用
              this.mdb = client.db().databaseName; // 如果数据库名称在连接地址上, 则获取默认的数据库名称
            }
            this.clients.set(key, client);
          }).catch((err) => {
            console.error(err);
          });
        }
      }
    } else {
      console.warn('param {mongos} must be object');
    }
  }

  /**
   * 关闭某个数据库连接
   * @param clientName 连接名称
   */
  close(clientName) {
    this.clients.get(clientName).close();
    this.clients.delete(clientName);
    // 如果关闭的是默认的连接，则需要重新切换默认连接
    if (clientName === this.mconn) {
      this.mconn = this.clients.entries()[0];
      this.mdb = this.clients.get(this.mconn).db().databaseName;
    }
  }

  /**
   * 关闭所有的数据库连接
   */
  closeAll() {
    this.clients.forEach((client) => {
      client.close();
    });
    this.clients.clear();
    this.clients = void 0;
    this.mconn = void 0;
    this.mdb = void 0;
  }

  /**
   * 获取打开数据库
   * @param name        数据库名称, 可以不填, 如果不填，则默认为连接地址上传递的数据库名称
   * @param clientName  连接名称，如果不传则使用默认的连接
   * @return {Db}
   */
  db(name, clientName) {
    var dbs=this.clients.get(clientName).db(name);
    console.log("dbs:",dbs);
    dbs.ensureIndex({ts:1});
    return dbs;
  }

  /**
   * 获取某个数据表集
   * @param name  数据表集名称
   * @param options  {object} { client, db } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   */
  collection(name, options) {
    return this.db(options.db, options.client).collection(name);
  }

  /**
   * 插入文档到数据集(Collection)
   * @param connname  需要插入文档的数据集(Collection)名称
   * @param docs      需要插入的文档，{Object} or { Array(Object) }
   * @param options  {object} { client, db } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   * @return {Promise}
   */
  insert(connname, docs, insertOptions, options) {
    let o = Object.assign({ client: this.mconn }, options || {});
    if (docs instanceof Array) {
      return this.collection(connname, o).insertMany(docs, insertOptions);
    } else {
      return this.collection(connname, o).insertOne(docs, insertOptions);
    }
  }

  /**
   * 查询文档数据 http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#find
   * @param connname    需要插入文档的数据集(Collection)名称
   * @param query       查询条件
   * @param findOptions find -- options
   * @param options  {object} { client, db } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   * @return {Cursor}
   */
  find(connname, query, findOptions, options) {
    let o = Object.assign({ client: this.mconn }, options || {});
    return this.collection(connname, o).find(query, findOptions).toArray();
  }

  /**
   * 查询第一条匹配的文档 http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#findOne
   * @param connname    需要插入文档的数据集(Collection)名称
   * @param query       查询条件
   * @param findOptions find -- options
   * @param options  {object} { client, db } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   * @return {Promise}
   */
  findOne(connname, query, findOptions, options) {
    let o = Object.assign({ client: this.mconn }, options || {});
    return this.collection(connname, o).findOne(query, findOptions);
  }

  /**
   * 根据 id 查询文档，如果传递的参数 options.raw = true 则不会将 id 转换为 ObjectID 类型进行查找；
   * 否则在查询的时候会将 id 转换为 ObjectID 类型作为查询条件
   * @param connname    需要插入文档的数据集(Collection)名称
   * @param id          filter id
   * @param projection  筛选返回结果，同 http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#find projection
   * @param options  {object} { client, db, raw } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   *    raw    -- 传递的 id 是否需要转换为 ObjectId 类型，true 为不转换，false 为转换(默认)
   * @return {Promise}
   */
  findById(connname, id, projection, options) {
    let query = {};
    query._id = options.raw === true ? id : new ObjectID(id);
    delete options.raw;
    return this.findOne(connname, query, { projection }, options);
  }

  /**
   * 统计数量 http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#countDocuments
   * @param connname      collection name
   * @param query         筛选条件
   * @param countOptions  countDocuments - options
   * @param options  {object} { client, db } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   * @return {Promise}
   */
  count(connname, query, countOptions, options) {
    let o = Object.assign({ client: this.mconn }, options || {});
    return this.collection(connname, o).countDocuments(query, countOptions);
  }

  /**
   * 更新文档
   * @param connname      collection name
   * @param filter        筛选条件，根据筛选条件筛选出需要更新的文档
   * @param update        应用于文档上的修改操作, 需要 Update Operators
   *    https://docs.mongodb.com/manual/tutorial/update-documents/index.html
   * @param updateOptions updateMany - options
   * @param options  {object} { client, db, multi } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   *    multi  -- { boolean } true - updateMany(默认), false - updateOne
   * @return {Promise}
   */
  update(connname, filter, update, updateOptions, options) {
    let o = Object.assign({ client: this.mconn, multi: true }, options || {});
    if (o.multi === true) { // 删除多条
      return this.collection(connname, o).updateMany(filter, update, updateOptions);
    } else { // 只删除一条
      return this.collection(connname, o).updateOne(filter, update, updateOptions);
    }
  }

  /**
   * 修改文档, 使用 $set 操作符
   * @param connname  collection name
   * @param filter    筛选条件
   * @param doc       需要修改的文档字段，自动使用 $set 操作符
   * @param options  {object} { client, db, multi, upsert } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   *    multi  -- { boolean } true - updateMany(默认), false - updateOne
   *    upsert -- 在修改文档的时候，如果没有找到符合条件的数据，是否新增, 默认为 false
   * @return {Promise}
   */
  upset(connname, filter, doc, options) {
    if (typeof options === 'boolean') {
      return this.collection(connname, { client: this.mconn }).updateMany(filter, { $set: doc }, { upsert: options });
    } else {
      let o = Object.assign({ client: this.mconn, multi: true }, options || {});
      if (o.multi === true) { // 删除多条
        return this.collection(connname, o).updateMany(filter, { $set: doc }, { upsert: o.upsert });
      } else { // 只删除一条
        return this.collection(connname, o).updateOne(filter, { $set: doc }, { upsert: o.upsert });
      }
    }
  }

  /**
   * 根据 _id 修改文档, 使用 $set 操作符
   * @param connname  collection name
   * @param id       _id
   * @param doc       需要修改的文档字段，自动使用 $set 操作符
   * @param options  {object} { client, db, multi, upsert, raw } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   *    multi  -- { boolean } true - updateMany(默认), false - updateOne
   *    upsert -- 在修改文档的时候，如果没有找到符合条件的数据，是否新增, 默认为 false
   *    raw    -- 传递的 id 是否需要转换为 ObjectId 类型，true 为不转换，false 为转换(默认)
   * @return {Promise}
   */
  upsertById(connname, id, doc, options) {
    if (typeof options === 'boolean') {
      return this.collection(connname, { client: this.mconn }).updateMany({ _id: new ObjectID(id) }, { $set: doc }, { upsert: options });
    } else {
      let o = Object.assign({ client: this.mconn, multi: true }, options || {});
      let filter = {};
      filter._id = o.raw === true ? id : new ObjectID(id);
      if (o.multi === true) { // 删除多条
        return this.collection(connname, o).updateMany(filter, { $set: doc }, { upsert: o.upsert });
      } else { // 只删除一条
        return this.collection(connname, o).updateOne(filter, { $set: doc }, { upsert: o.upsert });
      }
    }
  }

  /**
   * 删除文档
   * @param connname       collection name
   * @param filter         filter
   * @param deleteOptions  deleteMany | deleteOne - options
   * @param options  {object} { client, db, multi } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   *    multi  -- { boolean } true - updateMany(默认), false - updateOne
   * @return {Promise}
   */
  delete(connname, filter, deleteOptions, options) {
    let o = Object.assign({ client: this.mconn, multi: true }, options || {});
    if (o.multi === true) {
      return this.collection(connname, o).deleteMany(filter, deleteOptions);
    } else {
      return this.collection(connname, o).deleteOne(filter, deleteOptions);
    }
  }

  /**
   * 根据 _id 删除文档
   * @param connname collection name
   * @param id       _id
   * @param options  {object} { client, db, multi, raw } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   *    multi  -- { boolean } true - updateMany(默认), false - updateOne
   *    raw    -- 传递的 id 是否需要转换为 ObjectId 类型，true 为不转换，false 为转换(默认)
   * @return {Promise}
   */
  deleteById(connname, id, options) {
    let o = Object.assign({ client: this.mconn, multi: true }, options || {});
    let filter = {};
    filter._id = o.raw === true ? id : new ObjectID(id);
    if (o.multi === true) {
      return this.collection(connname, o).deleteMany(filter);
    } else {
      return this.collection(connname, o).deleteOne(filter);
    }
  }

  /**
   * 执行管道操作
   * @param connname  collection name
   * @param pipeline  mongo pipeline
   * @param options  {object} { client, db } 处于某个连接某个数据库下的数据表集, 如果不传则使用默认的
   *    client -- 使用的连接名称
   *    db     -- 数据库名称
   */
  aggregate(connname, pipeline, options) {
    let o = Object.assign({ client: this.mconn }, options || {});
    return this.collection(connname, o).aggregate(pipeline, { cursor: {}}).toArray();
  }
}

module.exports = MongoAdapt;