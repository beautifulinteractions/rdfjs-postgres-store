
'use strict';

const Store = require('.');
const stream = require('stream');
const factory = require('rdf-data-model');
const asynctools = require('asynctools');
const ArrayIterator = require('asynciterator').ArrayIterator;

function streamToArray(readStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readStream
      .on('data', (chunk) => { chunks.push(chunk); })
      .on('end', () => { resolve(chunks); })
      .on('error', (err) => { reject(err); });
  });
}

(async () => {

  const connectionString = 'postgres://127.0.0.1/rdfjs-store';
  const store = new Store(connectionString);

  await store.dropTables();
  await store.createTables();

  const quads = [];
  for (let i = 0; i < 10000; i++) {
    quads.push(factory.quad(
      factory.namedNode('http://ex.com/s' + i),
      factory.namedNode('http://ex.com/p' + Math.floor(i % 4)),
      factory.literal('' + i, 'http://www.w3.org/2001/XMLSchema#integer'),
      factory.namedNode('http://ex.com/g')
    ));
  }

  const preImport = Date.now();
  await asynctools.onEvent(store.import(new ArrayIterator(quads)), 'end');
  const postImport = Date.now();

  console.log('IMPORT TIME', (postImport - preImport) / 1000);

  const preRemove = Date.now();
  await asynctools.onEvent(store.removeMatches(), 'end');
  const postRemove = Date.now();

  console.log('REMOVE TIME', (postRemove - preRemove) / 1000);

  // const count = await store.estimate(null, null, [
  //   { test: 'gt', comparate: factory.literal('5', 'http://www.w3.org/2001/XMLSchema#integer') },
  //   { test: 'lt', comparate: factory.literal('10', 'http://www.w3.org/2001/XMLSchema#integer') }
  // ]);
  // const found = await streamToArray(store.match(null, null, [
  //   { test: 'gt', comparate: factory.literal('5', 'http://www.w3.org/2001/XMLSchema#integer') },
  //   { test: 'lt', comparate: factory.literal('10', 'http://www.w3.org/2001/XMLSchema#integer') }
  // ]));
  //
  // console.log('COUNT', count, found.length);
  //
  // await asynctools.onEvent(store.removeMatches(null, factory.namedNode('http://ex.com/p1')), 'end');
  //
  // console.log('COUNT2', await store.estimate());
  //
  // console.log('Done');

  await store.close();

})();

