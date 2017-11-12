
'use strict';

const AsyncTransformIterator = require('./AsyncTransformIterator');

class EdgeDeleterIterator extends AsyncTransformIterator {

  constructor(store, source, options) {
    super(source, options);
    if (!options) options = {};
    this._batch = options.batch || 20;
    this._store = store;
    this._buffer = [];
  }

  async _asyncTransform(edge) {
    const store = this._store;
    const buffer = this._buffer;
    buffer.push(edge);
    if (buffer.length === this._batch) {
      this._buffer = [];
      try {
        await store._deleteManyEdges(buffer);
      } catch (err) {
        this.emit('error', err);
      }
    }
  }

  async _asyncFlush() {
    const store = this._store;
    const buffer = this._buffer;
    if (buffer.length > 0) {
      try {
        await store._deleteManyEdges(buffer);
      } catch (err) {
        this.emit('error', err);
      }
    }
  }

}

module.exports = EdgeDeleterIterator;
