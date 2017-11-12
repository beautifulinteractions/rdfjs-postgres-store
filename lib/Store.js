
'use strict';

const knex = require('knex');
const utils = require('./utils');
const nanoid = require('nanoid');
const factory = require('rdf-data-model');
const Promise = require('bluebird');
const EventEmitter = require('events').EventEmitter;
const ResolveEndEmitter = require('./ResolveEndEmitter');
const EdgeDeleterIterator = require('./EdgeDeleterIterator');
const QuadRemoverIterator = require('./QuadRemoverIterator');
const QuadImporterIterator = require('./QuadImporterIterator');
const VertexDeleterIterator = require('./VertexDeleterIterator');
const QuadMaterializerIterator = require('./QuadMaterializerIterator');


/**
 * Type definition for instances of RDF/JS Quad
 * @typedef {Object} Quad
 */

/**
 * Type definition for instances of RDF/JS Term
 * @typedef {Object} Term
 */

/**
 * Type definition for raw vertex objects
 * @typedef {Object} Vertex
 */

/**
 * Type definition for raw edge objects
 * @typedef {Object} Edge
 */


/**
 *
 */
class Store extends EventEmitter {

  /**
   *
   * @param {String} connectionString
   * @param {Object} [options]
   */
  constructor(connectionString, options) {
    super();
    this._knex = knex({
      client: 'pg',
      connection: connectionString
    });
  }

  /**
   *
   * @param {Term} [subject]
   * @param {Term} [predicate]
   * @param {Term} [object]
   * @param {Term} [graph]
   * @returns {Promise.<Number>}
   */
  async countEstimate(subject, predicate, object, graph) {
    const estimatedTotalCount = (await this._knex.select()
      .column(this._knex.raw('reltuples::bigint AS estimate'))
      .from('pg_class')
      .where(this._knex.raw('oid = to_regclass(\'edges\')')))[0].estimate;
    let query = this._knex.count('edges.*')
      .from(estimatedTotalCount > 1000000 ? this._knex.raw('"edges" tablesample system (1)') : 'edges')
      .innerJoin('vertexes as subjects', 'subject', '=', 'subjects.id' )
      .innerJoin('vertexes as predicates', 'predicate', '=', 'predicates.id' )
      .innerJoin('vertexes as objects', 'object', '=', 'objects.id' )
      .innerJoin('vertexes as graphs', 'graph', '=', 'graphs.id' );
    if (subject) this._addWhereClausesForTerm(query, 'subject', subject);
    if (predicate) this._addWhereClausesForTerm(query, 'predicate', predicate);
    if (object) this._addWhereClausesForTerm(query, 'object', object);
    if (graph) this._addWhereClausesForTerm(query, 'graph', graph);
    return (await query)[0].count;
  }

  /**
   * Implementation of RDF/JS Source match()
   * @param subject {Term}
   * @param predicate {Term}
   * @param object {Term}
   * @param graph {Term}
   */
  match(subject, predicate, object, graph) {
    // use postgre's row_to_json() to return all vertexes pre-mapped to their
    // respective terms' names
    const query = this._knex.select()
      .column(this._knex.raw('row_to_json(subjects.*) as subject'))
      .column(this._knex.raw('row_to_json(predicates.*) as predicate'))
      .column(this._knex.raw('row_to_json(objects.*) as object'))
      .column(this._knex.raw('row_to_json(graphs.*) as graph'))
      .from('edges')
      .innerJoin('vertexes as subjects', 'subject', '=', 'subjects.id' )
      .innerJoin('vertexes as predicates', 'predicate', '=', 'predicates.id' )
      .innerJoin('vertexes as objects', 'object', '=', 'objects.id' )
      .innerJoin('vertexes as graphs', 'graph', '=', 'graphs.id' );
    if (subject) this._addWhereClausesForTerm(query, 'subject', subject);
    if (predicate) this._addWhereClausesForTerm(query, 'predicate', predicate);
    if (object) this._addWhereClausesForTerm(query, 'object', object);
    if (graph) this._addWhereClausesForTerm(query, 'graph', graph);
    return new QuadMaterializerIterator(this, query.stream());
  }

  /**
   * Implementation of RDF/JS Sink import()
   * @param {ReadableStream} source
   * @param {Object} [options]
   * @returns {}
   */
  import(source, options) {
    return new QuadImporterIterator(this, source, options);
  }

  remove(source, options) {
    return new QuadRemoverIterator(this, source, options);
  }

  removeMatches(subject, predicate, object, graph, options) {
    const store = this;
    const selectQuery = this._knex.select()
      .column('edges.id')
      .from('edges')
      .innerJoin('vertexes as subjects', 'subject', '=', 'subjects.id' )
      .innerJoin('vertexes as predicates', 'predicate', '=', 'predicates.id' )
      .innerJoin('vertexes as objects', 'object', '=', 'objects.id' )
      .innerJoin('vertexes as graphs', 'graph', '=', 'graphs.id' );
    if (subject) store._addWhereClausesForTerm(selectQuery, 'subject', subject);
    if (predicate) store._addWhereClausesForTerm(selectQuery, 'predicate', predicate);
    if (object) store._addWhereClausesForTerm(selectQuery, 'object', object);
    if (graph) store._addWhereClausesForTerm(selectQuery, 'graph', graph);
    return new EdgeDeleterIterator(this, selectQuery.stream(), options);
  }

  async _deleteManyEdges(edges) {
    await this._knex.delete()
      .from('edges')
      .whereIn('id', edges.map(edge => edge.id));
  }

  async _deleteManyVertexes(vertexes) {
    await this._knex.delete()
      .from('vertexes')
      .whereIn('id', vertexes.map(vertex => vertex.id));
  }

  /**
   *
   * @param {Term} term
   * @returns {EventEmitter}
   */
  deleteGraph(term) {
    const store = this;
    return new ResolveEndEmitter(this._knex.transaction(async (trx) => {
      let vertex = store._getCachedRawVertex(term);
      if (!vertex) {
        vertex = (await trx.select()
          .from('vertexes')
          .where('value', '=', term.value)
          .where('termType', '=', term.termType)
        )[0];
      }
      if (vertex) {
        await trx.delete().from('edges').where('graph', '=', vertex.id);
      }
    }));
  }

  /**
   * Translates a vertex into a RDF/JS Term instance
   * @param {Vertex} vertex
   * @returns {Term}
   * @private
   */
  _materializeTerm(vertex) {
    let term;
    switch(vertex.termType) {
      case 'NamedNode':
        term = factory.namedNode(vertex.value);
        break;
      case 'BlankNode':
        term = factory.blankNode(vertex.value);
        break;
      case 'Literal':
        term = factory.literal(vertex.value, (vertex.datatype && factory.namedNode(vertex.datatype)) || vertex.language);
        break;
      case 'DefaultGraph':
        term = factory.defaultGraph();
        break;
      default:
        throw new Error('Unsupported term type');
    }
    return term;
  }

  /**
   * Translates a dictionary of named vertexes into a RDF/JS Quad instance
   * @param {Object} vertexes
   * @returns {Quad}
   * @private
   */
  _materializeQuad(vertexes) {
    return factory.quad(
      this._materializeTerm(vertexes.subject),
      this._materializeTerm(vertexes.predicate),
      this._materializeTerm(vertexes.object),
      this._materializeTerm(vertexes.graph)
    );
  }

  async _insertQuads(quads, termVertexCache) {
    const terms = new Array(quads.length * 4);
    for (let i = 0; i < quads.length; i++) {
      terms[(i * 4) + 0] = quads[i].subject;
      terms[(i * 4) + 1] = quads[i].predicate;
      terms[(i * 4) + 2] = quads[i].object;
      terms[(i * 4) + 3] = quads[i].graph;
    }
    const vertexes = await this._insertManyTerms(this._knex, termVertexCache, terms);
    const edges = new Array(quads.length);
    for (let i = 0; i < quads.length; i++) {
      edges[i] = {
        subject: vertexes[(i * 4) + 0].id,
        predicate: vertexes[(i * 4) + 1].id,
        object: vertexes[(i * 4) + 2].id,
        graph: vertexes[(i * 4) + 3].id
      }
    }
    await this._insertManyEdges(this._knex, edges);
  }


  /**
   * Inserts a single edge into the store
   * @param trx
   * @param edge
   * @returns {Promise.<void>}
   * @private
   */
  async _insertOneEdge(trx, edge) {
    try {
      await trx.insert(edge).into('edges');
    } catch (err) {
      // if failure is not due to a violation of a unique constraint let the
      // error bubble up
      if (err.code !== '23505') {
        throw err;
      }
    }
  }

  /**
   * Inserts multiple edges into the store
   * @param trx
   * @param edges
   * @returns {Promise.<void>}
   * @private
   */
  async _insertManyEdges(trx, edges) {
    const store = this;
    try {
      // insert all edges at the same time
      await trx.insert(edges).into('edges');
    } catch (err) {
      // if failure is due to a violation of a unique constraint insert each
      // edge separately, otherwise let the error bubble up
      if (err.code === '23505') {
        await Promise.each(edges, rawEdge => store._insertOneEdge(trx, rawEdge));
      } else {
        throw err;
      }
    }
  }

  /**
   * Inserts multiple vertexes.
   * @param trx
   * @param {TermVertexCache} termVertexCache
   * @param terms
   * @returns {Promise.<Array>} array of raw vertexes
   * @private
   */
  async _insertManyTerms(trx, termVertexCache, terms) {
    const rawVertexesToInsert = [];
    const rawVertexes = new Array(terms.length);
    for (let i = 0; i < terms.length; i++) {
      const term = terms[i];
      rawVertexes[i] = termVertexCache.get(term);
      if (!rawVertexes[i]) {
        const rawVertexToInsert = {
          termType: term.termType,
          value: term.value
        };
        if (term.termType === 'Literal') {
          if (term.datatype) {
            rawVertexToInsert.datatype = term.datatype.value;
          } else if (term.language) {
            rawVertexToInsert.language = term.language;
          }
          const parsedValue = utils.parseLiteralValue(term);
          if (parsedValue) {
            rawVertexToInsert[`value_${parsedValue.type}`] = parsedValue.value;
          }
        }
        rawVertexesToInsert.push(rawVertexToInsert);
      }
    }
    if (rawVertexesToInsert.length > 0) {
      const insertedRawVertexes = await this._insertManyVertexes(trx, rawVertexesToInsert);
      for (let i = 0, j = 0; i < terms.length; i++) {
        if (!rawVertexes[i]) {
          rawVertexes[i] = insertedRawVertexes[j++];
          termVertexCache.set(terms[i], rawVertexes[i]);
        }
      }
    }
    return rawVertexes;
  }



  /**
   *
   * @param trx
   * @param vertexes
   * @returns {Promise.<Array.<Vertex>>}
   * @private
   */
  async _insertManyVertexes(trx, vertexes) {
    const store = this;
    try {
      return await trx.insert(vertexes).into('vertexes').returning('*');
    } catch (err) {
      if (err.code === '23505') {
        return await Promise.map(vertexes, vertex => this._insertOneVertex(trx, vertex));
      } else {
        throw err;
      }
    }
  }

  async _insertOneVertex(trx, vertex) {
    try {
      return (await trx.insert(vertex).into('vertexes').returning('*'))[0];
    } catch (err) {
      if (err.code === '23505') {
        const query = trx.select('*')
          .from('vertexes')
          .where('value', '=', vertex.value)
          .where('termType', '=', vertex.termType)
          .limit(1);
        if (vertex.termType === 'Literal') {
          if (vertex.datatype) {
            query.where('datatype', '=', vertex.datatype);
          } else if (vertex.language) {
            query.where('language', '=', vertex.language);
          }
        }
        return (await query)[0];
      } else {
        throw err;
      }
    }
  }

  vacuum() {
    const selectQuery = this._knex.select()
      .column('vertexes.*')
      .from('vertexes')
      .leftJoin('edges as edges_where_subject', 'vertexes.id', '=', 'edges_where_subject.subject')
      .leftJoin('edges as edges_where_predicate', 'vertexes.id', '=', 'edges_where_predicate.predicate')
      .leftJoin('edges as edges_where_object', 'vertexes.id', '=', 'edges_where_object.object')
      .leftJoin('edges as edges_where_graph', 'vertexes.id', '=', 'edges_where_graph.graph')
      .whereNull('edges_where_subject.id')
      .whereNull('edges_where_predicate.id')
      .whereNull('edges_where_object.id')
      .whereNull('edges_where_graph.id');
    const selectStream = selectQuery.stream();
    const deleterIterator = new VertexDeleterIterator(this, selectStream, {batch: 10});
    // Not quite sure why this is necessary but although the stream ends, the
    // iterator does not.
    // @TODO sort this out
    selectStream.on('end', () => {
      setTimeout(() => {
        deleterIterator._end();
      }, 10);
    });
    return deleterIterator;

  }

  _addWhereClausesForTerm(query, termName, term) {
    if (Array.isArray(term)) {
      for (const filter of term) {
        this._addWhereClausesForFilter(query, termName, filter);
      }
      console.log(query.toString());
    } else if (term.termType) {
      query
        .where(`${termName}s.termType`, '=', term.termType)
        .where(`${termName}s.value`, '=', term.value);
      if (term.termType === 'Literal') {
        if (term.datatype) {
          query.where(`${termName}s.datatype`, '=', term.datatype.value);
        }
        if (term.language) {
          query.where(`${termName}s.language`, '=', term.language);
        }
      }
    } else {
      throw new Error(`Unsupported term type ${term.termType}`);
    }
  }

  _addWhereClausesForFilter(query, termName, filter) {
    const { test, comparate } = filter;
    const parsedComparateValue = utils.parseLiteralValue(comparate);
    if (!parsedComparateValue) {
      throw new Error(`Could not parse term "${comparate.value}" of type "${comparate.datatype.value}"`);
    }
    const operator = ({ gt: '>', lt: '<', gte: '>=', lte: '<=', eq: '=', neq: '!=' })[test];
    if (!operator) {
      throw new Error(`Unsupported test "${test}" in filter.`);
    }
    query.where(`${termName}s.value_${parsedComparateValue.type}`, operator, parsedComparateValue.value);
  }

  async createTables() {
    return this._knex.transaction(async (trx) => {
      return trx.schema
        .createTableIfNotExists('vertexes', (table) => {
          table.bigincrements('id').primary();
          table.enum('termType', ['NamedNode', 'BlankNode', 'Literal', 'DefaultGraph']).notNullable();
          table.string('value').notNullable();
          table.string('language', 2);
          table.string('datatype');
          table.float('value_numeric');
          table.timestamp('value_datetime');
        })
        .raw(`CREATE UNIQUE INDEX "idx_unique_vertexes" ON "vertexes" ("termType", "value", coalesce("datatype", "language", ''))`)
        .createTableIfNotExists('edges', (table) => {
          table.bigincrements('id').primary();
          table.biginteger('subject').references('id').inTable('vertexes').notNullable();
          table.biginteger('predicate').references('id').inTable('vertexes').notNullable();
          table.biginteger('object').references('id').inTable('vertexes').notNullable();
          table.biginteger('graph').references('id').inTable('vertexes').notNullable();
        })
        .raw('CREATE UNIQUE INDEX "idx_unique_edges" ON "edges" ("subject", "predicate", "object", "graph")');
    });
  }

  async dropTables() {
    return this._knex.transaction(async (trx) => {
      return trx.schema
        .dropTableIfExists('edges')
        .dropTableIfExists('vertexes');
    });
  }

  async close() {
    return this._knex.destroy();
  }

}

module.exports = Store;