'use strict';

const EventEmitter = require('events').EventEmitter;

class ResolveEndEmitter extends EventEmitter {
  constructor(fnOrPromise, options) {
    super(options);
    const emitter = this;
    Promise.resolve(typeof(fnOrPromise) === 'function' ? fnOrPromise() : fnOrPromise)
      .then(() => { emitter.emit('end'); })
      .catch((err) => { emitter.emit('error', err); });
  }
}

module.exports = ResolveEndEmitter;
