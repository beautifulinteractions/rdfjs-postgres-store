
'use strict';

const AsyncTransformIterator = require('./AsyncTransformIterator');

class QuadRemoverIterator extends AsyncTransformIterator {

  constructor(store, source, options) {
    super(source, options);
    this._store = store;
  }

  async _asyncTransform(quad) {
    const store = this._store;
    await store.removeMatches(quad.subject, quad.predicate, quad.object, quad.graph);
  }

}

module.exports = QuadRemoverIterator;
