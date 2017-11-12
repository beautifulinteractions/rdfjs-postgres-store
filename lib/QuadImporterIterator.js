
'use strict';

const asynctools = require('asynctools');
const TermVertexCache = require('./TermVertexCache');
const AsyncTransformIterator = require('./AsyncTransformIterator');

class QuadImporterIterator extends AsyncTransformIterator {

  constructor(store, source, options) {
    super(source, options);
    if (!options) options = {};
    this._batch = options.batch || 20;
    this._store = store;
    this._cache = new TermVertexCache();
    this._buffer = [];
  }

  async _asyncTransform(quad) {
    const store = this._store;
    const buffer = this._buffer;
    buffer.push(quad);
    if (buffer.length === this._batch) {
      this._buffer = [];
      try {
        await store._insertQuads(buffer, this._cache);
      } catch (err) {
        this.emit('error', err);
      }
    }
  }

  async _asyncFlush() {
    const store = this._store;
    const buffer = this._buffer;
    const termVertexCache = this._cache;
    if (buffer.length > 0) {
      try {
        await store._insertQuads(buffer, termVertexCache);
      } catch (err) {
        this.emit('error', err);
      }
    }
    termVertexCache.destroy();
  }

}

module.exports = QuadImporterIterator;