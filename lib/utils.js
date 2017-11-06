

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

function termToObject(term) {
  const obj = {
    termType: term.termType,
    value: term.value,
    datatype: (term.datatype && term.datatype.value) || undefined,
    language: term.language
  };
  if (obj.termType === 'Literal' && obj.datatype) {
    const parser = DATATYPE_TO_FLOAT_PARSERS[obj.datatype];
    if (parser) obj.floatValue = parser(obj);
  }
  return obj;
}

module.exports.termToObject = termToObject;

function materializeTerm(term) {
  let materialized;
  switch(term.termType) {
    case 'NamedNode':
      materialized = factory.namedNode(term.value);
      break;
    case 'Literal':
      materialized = factory.literal(term.value, term.language || (term.datatype && factory.namedNode(term.datatype)) || null);
      break;
    case 'DefaultGraph':
      materialized = factory.defaultGraph();
      break;
    case 'BlankNode':
      materialized = factory.blankNode(term.value);
      break;
    default:
      throw new Error(`Unsupported termType ${term.termType}`);
  }
  return materialized;
}

module.exports.materializeTerm = materializeTerm;

function materializeQuad(quad) {
  return factory.quad(
    materializeTerm(quad.subject),
    materializeTerm(quad.predicate),
    materializeTerm(quad.object),
    materializeTerm(quad.graph)
  );
}

module.exports.materializeQuad = materializeQuad;

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

class QuadImporterIterator extends TransformIterator {
  constructor(store, source, options) {
    super(source, options);
    this._store = store;
  }
  _transform(quad, done) {
    const iterator = this;
    this._store._insertQuad(quad)
      .then(() => {
        done();
      })
      .catch((err) => {
        iterator.emit('error', err);
        done();
      });
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
    this._store._deleteQuad(quad)
      .then(() => {
        done();
      })
      .catch((err) => {
        iterator.emit('error', err);
        done();
      });
  }
}

module.exports.QuadRemoverIterator = QuadRemoverIterator;

