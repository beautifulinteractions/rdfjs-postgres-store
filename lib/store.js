
'use strict';

const knex = require('knex');
const utils = require('./utils');
const nanoid = require('nanoid');
const EventEmitter = require('events').EventEmitter;

class Store extends EventEmitter {

  constructor(connectionString, options) {
    super();
    this._knex = knex({
      client: 'pg',
      connection: connectionString
    });
  }

  async estimate(subject, predicate, object, graph) {
    const estimatedTotalCount = (await this._knex.select()
      .column(this._knex.raw('reltuples::bigint AS estimate'))
      .from('pg_class')
      .where(this._knex.raw('oid = to_regclass(\'quads\')')))[0].estimate;
    let query = this._knex.count('quads.*')
      .from(estimatedTotalCount > 1000000 ? this._knex.raw('"quads" tablesample system (1)') : 'quads');
    if (subject) this._addWhereClausesForTerm(query, 'subject', subject);
    if (predicate) this._addWhereClausesForTerm(query, 'predicate', predicate);
    if (object) this._addWhereClausesForTerm(query, 'object', object);
    if (graph) this._addWhereClausesForTerm(query, 'graph', graph);
    return (await query)[0].count;
  }

  match(subject, predicate, object, graph) {
    const query = this._knex.select()
      .from('quads');
    if (subject) this._addWhereClausesForTerm(query, 'subject', subject);
    if (predicate) this._addWhereClausesForTerm(query, 'predicate', predicate);
    if (object) this._addWhereClausesForTerm(query, 'object', object);
    if (graph) this._addWhereClausesForTerm(query, 'graph', graph);
    return new utils.QuadMaterializerIterator(this, query.stream());
  }

  import(source, options) {
    return (options && options.batch)
      ? new utils.BatchImporterIterator(this, source, options)
      : new utils.QuadImporterIterator(this, source);
  }

  remove(source) {
    return new utils.QuadRemoverIterator(this, source);
  }

  removeMatches(subject, predicate, object, graph) {
    return this.remove(this.match(subject, predicate, object, graph));
  }

  deleteGraph(graph) {
    return this.removeMatches(null, null, null, graph);
  }

  async _insertQuad(quad) {
    try {
      await this._knex.insert(quad).into('quads');
    } catch (err) {
      if (err.code === '23505') {
        if (Array.isArray(quad)) {
          for (const _quad of quad) {
            await this._insertQuad(_quad);
          }
        }
      } else {
        throw err;
      }
    }
  }

  async _deleteQuad(quad) {
    await this._knex.delete().from('quads')
      .where('subject', '=', quad.subject)
      .where('predicate', '=', quad.predicate)
      .where('object', '=', quad.object)
      .where('graph', '=', quad.graph)
  }

  _addWhereClausesForTerm(query, termName, term) {
    if (term.termType) {
      query.where(termName, '=', utils.serializeTerm(term));
    } else if (Array.isArray(term)) {
      for (const filter of term) {
        this._addWhereClausesForFilter(query, termName, filter);
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
    query.where(`${termName}_${parsedComparateValue.type}`, operator, parsedComparateValue.value);
  }

  async createTables() {
    return this._knex.transaction(async (trx) => {
      return trx.schema
        .createTableIfNotExists('quads', (table) => {
          table.string('subject').notNullable();
          table.float('subject_numeric');
          table.timestamp('subject_datetime');
          table.string('predicate').notNullable();
          table.string('object').notNullable();
          table.float('object_numeric');
          table.timestamp('object_datetime');
          table.string('graph');
          table.unique(['subject', 'predicate', 'object', 'graph']);
        });
    });
  }

  async dropTables() {
    return this._knex.transaction(async (trx) => {
      return trx.schema
        .dropTableIfExists('quads');
    });
  }

  async close() {
    return this._knex.destroy();
  }

}

module.exports = Store;