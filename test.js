
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
  for (let i = 0; i < 200; i++) {
    quads.push(factory.quad(
      factory.namedNode('http://ex.com/s' + i),
      factory.namedNode('http://ex.com/p' + Math.floor(i % 4)),
      factory.literal('literal'),
      factory.namedNode('http://ex.com/g')
    ));
  }

  await asynctools.onEvent(store.import(new ArrayIterator(quads)), 'end');

  const count = await store.count(null, factory.namedNode('http://ex.com/p1'))
  const found = await streamToArray(store.match(null, factory.namedNode('http://ex.com/p1')));

  console.log('COUNT', count, found.length);

  await asynctools.onEvent(store.removeMatches(null, factory.namedNode('http://ex.com/p1')), 'end');

  console.log('COUNT2', await store.count());

  console.log('Done');

  await store.close();

})();

