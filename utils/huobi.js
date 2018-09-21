/**
 * Created by haoran.shu on 2018/7/27 18:18.
 */
const request = require('request');
const Agent = require('socks5-https-client/lib/Agent');
const HUOBI = 'https://api.huobipro.com';

class HttpError extends Error {
  constructor(url, message) {
    super(message);
    this.url = url;
  }
}

module.exports = function(opts, cb) {
  if(!opts.hasOwnProperty('uri')) {
    opts.url = HUOBI + opts.url;
  }
  opts.agentClass = Agent;
  opts.agentOptions = {
    socksHost: '127.0.0.1',
    socksPort: 1080
  };
  request(opts, function(err, res, body) {
    if(err) {
      cb(new HttpError(opts.url, err.message));
    } else {
      if(res.statusCode === 200) {
        cb(null, body);
      } else {
        cb(new HttpError(opts.url, res.statusCode + ' & ' + res.statusMessage));
      }
    }
  });
};
