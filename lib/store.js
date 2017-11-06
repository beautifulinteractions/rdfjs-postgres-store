
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
      .from(estimatedTotalCount > 1000000 ? this._knex.raw('"quads" tablesample system (1)') : 'quads')
      .innerJoin('terms AS subjects', 'quads.subject', 'subjects.id')
      .innerJoin('terms AS predicates', 'quads.predicate', 'predicates.id')
      .innerJoin('terms AS objects', 'quads.object', 'objects.id')
      .innerJoin('terms AS graphs', 'quads.graph', 'graphs.id');
    if (subject) this._addWhereClausesForTerm(query, 'subject', subject);
    if (predicate) this._addWhereClausesForTerm(query, 'predicate', predicate);
    if (object) this._addWhereClausesForTerm(query, 'object', object);
    if (graph) this._addWhereClausesForTerm(query, 'graph', graph);
    return (await query)[0].count;
  }

  match(subject, predicate, object, graph) {
    const query = this._initSelectQuery();
    query.column([
      this._knex.raw('row_to_json(subjects) AS subject'),
      this._knex.raw('row_to_json(predicates) AS predicate'),
      this._knex.raw('row_to_json(objects) AS object'),
      this._knex.raw('row_to_json(graphs) AS graph')
    ]);
    if (subject) this._addWhereClausesForTerm(query, 'subject', subject);
    if (predicate) this._addWhereClausesForTerm(query, 'predicate', predicate);
    if (object) this._addWhereClausesForTerm(query, 'object', object);
    if (graph) this._addWhereClausesForTerm(query, 'graph', graph);
    return new utils.QuadMaterializerIterator(this, query.stream());
  }

  import(source) {
    return new utils.QuadImporterIterator(this, source);
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

  _initSelectQuery(trx) {
    return (trx || this._knex).select()
      .from('quads')
      .innerJoin('terms AS subjects', 'quads.subject', 'subjects.id')
      .innerJoin('terms AS predicates', 'quads.predicate', 'predicates.id')
      .innerJoin('terms AS objects', 'quads.object', 'objects.id')
      .innerJoin('terms AS graphs', 'quads.graph', 'graphs.id');
  }

  async _insertQuad(quad) {
    await this._knex.transaction(async (trx) => {
      const trx = this._knex;
      const termsQuery = trx.insert([
        { id: nanoid(), ...utils.termToObject(quad.subject) },
        { id: nanoid(), ...utils.termToObject(quad.predicate) },
        { id: nanoid(), ...utils.termToObject(quad.object) },
        { id: nanoid(), ...utils.termToObject(quad.graph) }
      ]).into('terms');
      const termsIds = (await trx.raw('? on conflict on constraint terms_termtype_value_datatype_language_unique do update set "value" = "terms"."value" returning "terms"."id"', [
        termsQuery
      ])).rows;
      await trx.raw('? on conflict do nothing', [
        trx.insert({
          id: nanoid(),
          subject: termsIds[0].id,
          predicate: termsIds[1].id,
          object: termsIds[2].id,
          graph: termsIds[3].id,
        }).into('quads')
      ]);
    });
  }

  async _deleteQuad(quad) {
    const store = this;
    await this._knex.transaction(async (trx) => {
      const query = this._initSelectQuery(trx);
      query.column('quads.id');
      ['subject', 'predicate', 'object', 'graph'].forEach((termName) => {
        store._addWhereClausesForTerm(query, termName, utils.termToObject(quad[termName]));
      });
      const [ deleted ] = await trx.delete().from('quads').whereIn('id', query).returning('*');
      await trx.delete()
        .from('terms')
        .whereIn('id', [deleted.subject, deleted.predicate, deleted.object, deleted.graph])
        .whereNotExists(trx.select().from('quads').where('subject', '=', deleted.subject))
        .whereNotExists(trx.select().from('quads').where('predicate', '=', deleted.predicate))
        .whereNotExists(trx.select().from('quads').where('object', '=', deleted.object))
        .whereNotExists(trx.select().from('quads').where('graph', '=', deleted.graph));
    });
  }

  _addWhereClausesForTerm(query, termName, term) {
    if (term.termType) {
      query.where(`${termName}s.termType`, '=', term.termType);
      query.where(`${termName}s.value`, '=', term.value);
      if (term.language) query.where(`${termName}s.language`, '=', term.language);
      if (term.datatype) query.where(`${termName}s.datatype`, '=', term.datatype);
    } else if (Array.isArray(term)) {
      for (const filter of term) {
        this._addWhereClausesForFilter(query, termName, filter);
      }
    } else {
      throw new Error(`Unsupported term type ${term.termType}`);
    }
  }

  _addWhereClausesForFilter(query, termName, filter) {
    let { test, comparate } = filter;
    comparate = utils.termToObject(comparate);
    if (!comparate.floatValue && comparate.floatValue !== 0) {
      throw new Error(`Could not convert term "${comparate.value}" of type "${comparate.datatype}" to float`);
    }
    const operator = ({ gt: '>', lt: '<', gte: '>=', lte: '<=', eq: '=', neq: '!=' })[test];
    if (!operator) {
      throw new Error(`Unsupported test "${test}" in filter.`);
    }
    query.where(`${termName}s.floatValue`, operator, comparate.floatValue);
    query.where(`${termName}s.datatype`, '=', comparate.datatype);
  }

  async createTables() {
    return this._knex.transaction(async (trx) => {
      return trx.schema
        .createTableIfNotExists('terms', (termsTable) => {
          termsTable.string('id').primary();
          termsTable.enum('termType', [
            'NamedNode',
            'Literal',
            'BlankNode',
            'DefaultGraph'
          ]).notNullable();
          termsTable.string('value').notNullable();
          termsTable.float('floatValue');
          termsTable.string('datatype').notNullable().defaultsTo('');
          termsTable.string('language').notNullable().defaultsTo('');
          termsTable.unique(['termType', 'value', 'datatype', 'language']);
        })
        .createTableIfNotExists('quads', (quadsTable) => {
          quadsTable.string('id').primary();
          quadsTable.string('subject')
            .references('id')
            .inTable('terms')
            .notNullable();
          quadsTable.string('predicate')
            .references('id')
            .inTable('terms')
            .notNullable();
          quadsTable.string('object')
            .references('id')
            .inTable('terms')
            .notNullable();
          quadsTable.string('graph')
            .references('id')
            .inTable('terms')
            .notNullable();
          quadsTable.unique(['subject', 'predicate', 'object', 'graph']);
        });
    });
  }

  async dropTables() {
    return this._knex.transaction(async (trx) => {
      return trx.schema
        .dropTableIfExists('quads')
        .dropTableIfExists('terms');
    });
  }

  async close() {
    return this._knex.destroy();
  }

}

module.exports = Store;