
const n3u = require('n3').Util;
const factory = require('rdf-data-model');
const TransformIterator = require('asynciterator').TransformIterator;

const XSD = 'http://www.w3.org/2001/XMLSchema#';

function parseNumericLiteralToFloat(termObj) {
  return parseFloat(termObj.value);
}

function parseDatetimeLiteralToFloat(termObj) {
  return new Date(termObj.value).valueOf();
}

const DATATYPE_TO_FLOAT_PARSERS = {
  [XSD + 'byte']: parseNumericLiteralToFloat,
  [XSD + 'short']: parseNumericLiteralToFloat,
  [XSD + 'decimal']: parseNumericLiteralToFloat,
  [XSD + 'integer']: parseNumericLiteralToFloat,
  [XSD + 'int']: parseNumericLiteralToFloat,
  [XSD + 'long']: parseNumericLiteralToFloat,
  [XSD + 'negativeInteger']: parseNumericLiteralToFloat,
  [XSD + 'positiveInteger']: parseNumericLiteralToFloat,
  [XSD + 'nonNegativeInteger']: parseNumericLiteralToFloat,
  [XSD + 'nonPositiveInteger']: parseNumericLiteralToFloat,
  [XSD + 'unsignedLong']: parseNumericLiteralToFloat,
  [XSD + 'unsignedShort']: parseNumericLiteralToFloat,
  [XSD + 'unsignedInt']: parseNumericLiteralToFloat,
  [XSD + 'unsignedByte']: parseNumericLiteralToFloat,
  [XSD + 'date']: parseDatetimeLiteralToFloat,
  [XSD + 'dateTime']: parseDatetimeLiteralToFloat
};

function parseTermToFloat(term) {
  if (term.termType === 'Literal' && term.datatype) {
    const parser = DATATYPE_TO_FLOAT_PARSERS[term.datatype.value];
    if (parser) return parser(term);
  }
  return undefined;
}

module.exports.parseTermToFloat = parseTermToFloat;

const DEFAULT_GRAPH = 'DEFAULT_GRAPH';

function materializeTerm(term) {
  let materialized;
  if (term === DEFAULT_GRAPH) {
    materialized = factory.defaultGraph();
  } else if (n3u.isLiteral(term)) {
    const value = n3u.getLiteralValue(term);
    const datatype = n3u.getLiteralType(term);
    const language = n3u.getLiteralLanguage(term);
    materialized = factory.literal(value, language || (datatype && factory.namedNode(datatype)) || null);
  } else if (n3u.isBlank(term)) {
    materialized = factory.blankNode(term.slice(2));
  } else if (n3u.isIRI(term)) {
    materialized = factory.namedNode(term);
  } else {
    throw new Error(`Bad term "${term}", cannot export`);
  }
  return materialized;
}

function materializeQuad(quad) {
  return factory.quad(
    materializeTerm(quad.subject),
    materializeTerm(quad.predicate),
    materializeTerm(quad.object),
    materializeTerm(quad.graph)
  );
}

function serializeTerm(term) {
  let serialized;
  switch (term.termType) {
    case 'Literal':
      if (term.datatype) return n3u.createLiteral(term.value, serializeTerm(term.datatype));
      else if (term.language) return n3u.createLiteral(term.value, term.language);
      serialized = n3u.createLiteral(term.value);
      break;
    case 'NamedNode':
      serialized = term.value;
      break;
    case 'DefaultGraph':
      serialized = DEFAULT_GRAPH;
      break;
    case 'BlankNode':
      serialized = '_:' + term.value;
      break;
    default:
      throw new Error('Unsupported termType ' + term.termType);
  }
  return serialized;
}

module.exports.serializeTerm = serializeTerm;

function serializeQuad(quad) {
  return {
    subject: serializeTerm(quad.subject),
    subject_float: parseTermToFloat(quad.subject),
    predicate: serializeTerm(quad.predicate),
    object: serializeTerm(quad.object),
    object_float: parseTermToFloat(quad.object),
    graph: serializeTerm(quad.graph)
  };
}

module.exports.serializeQuad = serializeQuad;

class QuadMaterializerIterator extends TransformIterator {
  constructor(store, source, options) {
    super(source, options);
    this._store = store;
  }
  _transform(quad, done) {
    this._push(materializeQuad(quad));
    done();
  }
}

module.exports.QuadMaterializerIterator = QuadMaterializerIterator;

class BatchImporterIterator extends TransformIterator {
  constructor(store, source, options) {
    super(source, options);
    this._batch = options.batch || 10;
    this._store = store;
    this._buffer = [];
  }
  _transform(quad, done) {
    const iterator = this;
    const buffer = this._buffer;
    buffer.push(serializeQuad(quad));
    if (buffer.length === this._batch) {
      this._buffer = [];
      this._store._insertQuad(buffer)
        .then(() => { done(); })
        .catch((err) => { iterator.emit('error', err); done(); });
    } else {
      done();
    }
  }
  _flush(done) {
    const iterator = this;
    const buffer = this._buffer;
    if (buffer.length > 0) {
      this._store._insertQuad(buffer)
        .then(() => { done(); })
        .catch((err) => { iterator.emit('error', err); done(); });
    } else {
      done();
    }
  }
}

module.exports.BatchImporterIterator = BatchImporterIterator;

class QuadImporterIterator extends TransformIterator {
  constructor(store, source, options) {
    super(source, options);
    this._store = store;
  }
  _transform(quad, done) {
    const iterator = this;
    this._store._insertQuad(serializeQuad(quad))
      .then(() => { done(); })
      .catch((err) => { iterator.emit('error', err); done(); });
  }
}

module.exports.QuadImporterIterator = QuadImporterIterator;

class QuadRemoverIterator extends TransformIterator {
  constructor(store, source, options) {
    super(source, options);
    this._store = store;
  }
  _transform(quad, done) {
    const iterator = this;
    this._store._deleteQuad(serializeQuad(quad))
      .then(() => { done(); })
      .catch((err) => { iterator.emit('error', err); done(); });
  }
}

module.exports.QuadRemoverIterator = QuadRemoverIterator;

