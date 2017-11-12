
const n3u = require('n3').Util;
const debug = require('debug');
const factory = require('rdf-data-model');
const asynctools = require('asynctools');
const EventEmitter = require('events').EventEmitter;
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

function materializeN3Term(term) {
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

function materializeN3Quad(quad) {
  return factory.quad(
    materializeN3Term(quad.subject),
    materializeN3Term(quad.predicate),
    materializeN3Term(quad.object),
    materializeN3Term(quad.graph)
  );
}

// function materializeTerm(term) {
//   let materialized;
//   switch(term.termType) {
//     case 'NamedNode':
//       materialized = factory.namedNode(term.value);
//       break;
//     case 'BlankNode':
//       materialized = factory.blankNode(term.value);
//       break;
//     case 'Literal':
//       materialized = factory.literal(term.value, (term.datatype && factory.namedNode(term.datatype)) || term.language);
//       break;
//     case 'DefaultGraph':
//       materialized = factory.defaultGraph();
//       break;
//     default:
//       throw new Error('Unsupported term type');
//   }
//   return materialized;
// }
//
// function materializeQuad(rawVertexesByTermName) {
//   return factory.quad(
//     materializeTerm(rawVertexesByTermName.subject),
//     materializeTerm(rawVertexesByTermName.predicate),
//     materializeTerm(rawVertexesByTermName.object),
//     materializeTerm(rawVertexesByTermName.graph)
//   );
// }

// function serializeTerm(term) {
//   let serialized;
//   switch (term.termType) {
//     case 'Literal':
//       if (term.datatype) return n3u.createLiteral(term.value, serializeTerm(term.datatype));
//       else if (term.language) return n3u.createLiteral(term.value, term.language);
//       serialized = n3u.createLiteral(term.value);
//       break;
//     case 'NamedNode':
//       serialized = term.value;
//       break;
//     case 'DefaultGraph':
//       serialized = DEFAULT_GRAPH;
//       break;
//     case 'BlankNode':
//       serialized = '_:' + term.value;
//       break;
//     default:
//       throw new Error('Unsupported termType ' + term.termType);
//   }
//   return serialized;
// }
//
// module.exports.serializeTerm = serializeTerm;

// function serializeQuad(quad) {
//   const serialized = {
//     subject: serializeTerm(quad.subject),
//     predicate: serializeTerm(quad.predicate),
//     object: serializeTerm(quad.object),
//     graph: serializeTerm(quad.graph)
//   };
//   for (const termName of ['subject', 'object']) {
//     const parsedTermValue = parseLiteralValue(quad[termName]);
//     if(parsedTermValue) {
//       serialized[`${termName}_${parsedTermValue.type}`] = parsedTermValue.value;
//     }
//   }
//   return serialized;
// }
//
// module.exports.serializeQuad = serializeQuad;

class N3QuadMaterializerIterator extends TransformIterator {
  constructor(store, source, options) {
    super(source, options);
    this._store = store;
    this._transform = asynctools.toCallback(async function (quad) {
      this._push(materializeN3Quad(quad));
    });
  }
}

module.exports.N3QuadMaterializerIterator = N3QuadMaterializerIterator;

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

class AsyncEndEmitter extends EventEmitter {
  constructor(functionOrPromise, options) {
    super(options);
    const emitter = this;
    const promise = typeof(functionOrPromise) === 'function'
      ? functionOrPromise()
      : functionOrPromise;
    promise
      .then(() => { emitter.emit('end'); })
      .catch((err) => { emitter.emit('error', err); });
  }
}

module.exports.AsyncEndEmitter = AsyncEndEmitter;
