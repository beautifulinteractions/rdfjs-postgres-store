
'use strict';

const LruCache = require('lru-cache');

class TermVertexCache {

  constructor(length) {
    this._cache = LruCache(length || 100 * 1000);
  }

  get(term) {
    return this._cache.get(this._getTermKey(term));
  }

  set(term, vertex) {
    return this._cache.set(this._getTermKey(term), vertex);
  }

  destroy() {
    this._cache.reset();
  }

  _getTermKey(term) {
    return term.termType
      + term.value
      + ((term.datatype && term.datatype.value) || term.language);
  }

}

module.exports = TermVertexCache;
