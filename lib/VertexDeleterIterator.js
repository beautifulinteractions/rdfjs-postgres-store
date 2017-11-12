
'use strict';

const AsyncTransformIterator = require('./AsyncTransformIterator');

class VertexDeleterIterator extends AsyncTransformIterator {

  constructor(store, source, options) {
    super(source, options);
    this._batch = options.batch || 20;
    this._store = store;
    this._buffer = [];
  }

  async _asyncTransform(vertex) {
    const store = this._store;
    const buffer = this._buffer;
    buffer.push(vertex);
    if (buffer.length === this._batch) {
      this._buffer = [];
      try {

        await store._deleteManyVertexes(buffer);
      } catch (err) {
        console.log(err);
        this.emit('error', err);
      }
    }
  }

  async _asyncFlush() {
    const store = this._store;
    const buffer = this._buffer;
    if (buffer.length > 0) {
      try {
        await store._deleteManyVertexes(buffer);
      } catch (err) {
        console.log(err);
        this.emit('error', err);
      }
    }
    console.log('END FLUSH');
  }

}

module.exports = VertexDeleterIterator;
