
const n3u = require('n3').Util;
const factory = require('rdf-data-model');
const asynctools = require('asynctools');
const TransformIterator = require('asynciterator').TransformIterator;

const XSD = 'http://www.w3.org/2001/XMLSchema#';

function parseNumericLiteral(term) {
  return { type: 'numeric', value: parseFloat(term.value) };
}

function parseDatetimeLiteral(term) {
  return { type: 'datatype', value: term.value };
}

const LITERAL_DATATYPE_PARSERS = {
  [XSD + 'byte']: parseNumericLiteral,
  [XSD + 'short']: parseNumericLiteral,
  [XSD + 'decimal']: parseNumericLiteral,
  [XSD + 'integer']: parseNumericLiteral,
  [XSD + 'int']: parseNumericLiteral,
  [XSD + 'long']: parseNumericLiteral,
  [XSD + 'negativeInteger']: parseNumericLiteral,
  [XSD + 'positiveInteger']: parseNumericLiteral,
  [XSD + 'nonNegativeInteger']: parseNumericLiteral,
  [XSD + 'nonPositiveInteger']: parseNumericLiteral,
  [XSD + 'unsignedLong']: parseNumericLiteral,
  [XSD + 'unsignedShort']: parseNumericLiteral,
  [XSD + 'unsignedInt']: parseNumericLiteral,
  [XSD + 'unsignedByte']: parseNumericLiteral,
  [XSD + 'date']: parseDatetimeLiteral,
  [XSD + 'dateTime']: parseDatetimeLiteral
};

function parseLiteralValue(term) {
  if (term.termType === 'Literal' && term.datatype) {
    const parser = LITERAL_DATATYPE_PARSERS[term.datatype.value];
    if (parser) {
      return parser(term);
    }
  }
  return undefined;
}

module.exports.parseLiteralValue = parseLiteralValue;

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
  const serialized = {
    subject: serializeTerm(quad.subject),
    predicate: serializeTerm(quad.predicate),
    object: serializeTerm(quad.object),
    graph: serializeTerm(quad.graph)
  };
  for (const termName of ['subject', 'object']) {
    const parsedTermValue = parseLiteralValue(quad[termName]);
    if(parsedTermValue) {
      serialized[`${termName}_${parsedTermValue.type}`] = parsedTermValue.value;
    }
  }
  return serialized;
}

module.exports.serializeQuad = serializeQuad;

class QuadMaterializerIterator extends TransformIterator {
  constructor(store, source, options) {
    super(source, options);
    this._store = store;
    this._transform = asynctools.toCallback(async function (quad) {
      this._push(materializeQuad(quad));
    });
  }
}

module.exports.QuadMaterializerIterator = QuadMaterializerIterator;

class BatchImporterIterator extends TransformIterator {
  constructor(store, source, options) {
    super(source, options);
    this._batch = options.batch || 10;
    this._store = store;
    this._buffer = [];
    this._transform = asynctools.toCallback(async function (quad) {
      const buffer = this._buffer;
      buffer.push(serializeQuad(quad));
      if (buffer.length === this._batch) {
        this._buffer = [];
        try {
          await this._store._insertQuad(buffer);
        } catch (err) {
          this.emit('error', err);
        }
      }
    });
    this._flush = asynctools.toCallback(async function () {
      const buffer = this._buffer;
      if (buffer.length > 0) {
        try {
          await this._store._insertQuad(buffer);
        } catch (err) {
          this.emit('error', err);
        }
      }
    });
  }

}

module.exports.BatchImporterIterator = BatchImporterIterator;

class QuadImporterIterator extends TransformIterator {
  constructor(store, source, options) {
    super(source, options);
    this._store = store;
    this._transform = asynctools.toCallback(async function (quad) {
      try {
        await this._store._insertQuad(serializeQuad(quad));
      } catch (err) {
        this.emit('error', err);
      }
    });
  }
}

module.exports.QuadImporterIterator = QuadImporterIterator;

class QuadRemoverIterator extends TransformIterator {
  constructor(store, source, options) {
    super(source, options);
    this._store = store;
    this._transform = asynctools.toCallback(async function (quad) {
      try {
        await this._store._deleteQuad(serializeQuad(quad));
      } catch (err) {
        this.emit('error', err);
      }
    });
  }
}

module.exports.QuadRemoverIterator = QuadRemoverIterator;

