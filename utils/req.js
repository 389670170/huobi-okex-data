/**
 * 使用 Promise 包装 request 请求
 * Created by haoran.shu on 2018/7/27 16:24.
 */
const request = require('request');
const Agent = require('socks5-https-client/lib/Agent');
const HUOBI = 'https://api.huobipro.com';

let req = function(opts) {
  return new Promise((resolve, reject) => {
    request(opts, (err, res, body) => {
      if(err) {
        reject(new Error(opts.url + ' & ' + err.message));
      } else {
        if(res.statusCode === 200) {
          resolve(body);
        } else {
          reject(new Error(res.statusCode + ' & ' + res.statusMessage + ' & ' + opts.url));
        }
      }
    });
  });
};

// 请求火币接口
let hr = function(opts) {
  opts.url = HUOBI + opts.url;
  opts.agentClass = Agent;
  opts.agentOptions = {
    socksHost: '127.0.0.1',
    socksPort: 1080
  };
  return req(opts);
};

module.exports = hr;
