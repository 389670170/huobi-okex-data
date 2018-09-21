/**
 * Created by haoran.shu on 2018/7/27 10:06.
 */
const hb = require('./utils/huobi');
const mongo = require('./dbs/mongo');

// 配置 log4js
const log4js = require('log4js');
log4js.configure({
  appenders: {
    console: { type: 'console' },
    file: { type: 'dateFile', filename: '/tmp/huobi-sync.log', keepFileExt: true },
    'just-file': {type: 'logLevelFilter', appender: 'file', level: 'info'}
  },
  categories: { default: { appenders: ['console', 'just-file'], level: 'debug' } }
});
const logger = log4js.getLogger('huobi');

let failedTrades = new Map(); // 交易记录获取失败的 url 集合
let fsize = findex = 0;

// 根据币种组装数据库名称
function dbName(currency) {
  currency = currency.toUpperCase();
  if(currency === 'USDT') {
    return 'Tradedata'
  } else {
    return 'Tradedata' + currency;
  }
}
var oneSecond = 1000*60*1;
setInterval(async(req,res)=> {
// 递归反复同步失败列表的数据
function syncSet() {
  fsize = failedTrades.size;
  if(fsize === 0) { // 数据同步成功
    fsize = void 0;
    findex = void 0;
    logger.info('Synchronize success!');
  } else {
    findex = 0;
    for(let [symbol, value] of failedTrades) {
      findex++;
      hb({
        url: value.url
      }, (err, tradeText) => {
        findex++;
        if(!err) { // 请求交易记录成功
          failedTrades.delete(symbol); // 成功后, 删除失败记录
          tradeText = tradeText.replace(/id/g, '_id');
          let records = JSON.parse(tradeText).data[0].data;
          mongo.insert(symbol, records, { ordered: false }, { db: dbName(value.currency) }).catch(err => {});
        } else {
          logger.debug('err');
        }
        if(fsize === findex) {
          syncSet(); // 递归
        }
      });
    }
  }
}

// 1. 获取Pro站支持的所有交易对
hb({
  url: '/v1/common/symbols'
}, (err, symbolsText) => {
  if(err) {
    console.error(err);
  } else {
    let symbols = JSON.parse(symbolsText).data;
    for(let i = 0, len = symbols.length; i < len; i++) { // 遍历获取每一个币种的交易记录
      let symbol = symbols[i].symbol;
      hb({
        url: '/market/history/trade?size=2000&symbol=' + symbol
      }, (err, tradeText) => {
        if(err) {
          failedTrades.set(symbol, {
            currency: symbols[i]['quote-currency'],
            url: err.url
          });
        } else {
          //console.log("2000````````````````````````````");
          tradeText = tradeText.replace(/id/g, '_id');
          let records = JSON.parse(tradeText).data;
          console.log(JSON.parse(tradeText).data);
          console.log(symbol);
          console.log(records);
          mongo.insert(symbol, records, { ordered: false }, { db: dbName(symbols[i]['quote-currency']) }).catch(err => {});
        }
        if(i === len - 1) {
          syncSet(); // 同步失败的数据
        }
      });

    }
  }
});
}, oneSecond);