
'use strict';

const AsyncTransformIterator = require('./AsyncTransformIterator');

class QuadMaterializerIterator extends AsyncTransformIterator {

  constructor(store, source, options) {
    super(source, options);
    this._store = store;
  }

  async _asyncTransform(vertexes) {
    this._push(this._store._materializeQuad(vertexes));
  }

}

module.exports = QuadMaterializerIterator;
