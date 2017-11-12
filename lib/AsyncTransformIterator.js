
'use strict';

const asynctools = require('asynctools');
const TransformIterator = require('asynciterator').TransformIterator;

class AsyncTransformIterator extends TransformIterator {

  constructor(source, options) {
    super(source, options);
    this._transform = asynctools.toCallback(this._asyncTransform);
    this._flush = asynctools.toCallback(this._asyncFlush);
  }

  async _asyncTransform(item) {
    this._push(item);
  }

  async _asyncFlush() {
  }

}

module.exports = AsyncTransformIterator;
