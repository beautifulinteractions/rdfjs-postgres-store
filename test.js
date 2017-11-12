
'use strict';

const n3 = require('n3');
const fs = require('fs');
const Store = require('.');
const utils = require('./lib/utils');
const stream = require('stream');
const factory = require('rdf-data-model');
const asynctools = require('asynctools');
const ArrayIterator = require('asynciterator').ArrayIterator;


function createRdfReadStream(filePath, fileFormat) {
  return fs.createReadStream(filePath, 'utf8').pipe(new n3.StreamParser({ format: fileFormat }));
}

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

  const rdfReadStream = createRdfReadStream('/Users/jacopo/Downloads/21million.rdf.100000', 'text/turtle');
  const rdfIterator = new utils.N3QuadMaterializerIterator(null, rdfReadStream);

  console.log('STARTED');
  const preImport = Date.now();
  await asynctools.waitForEvent(store.import(rdfIterator), 'end');
  const postImport = Date.now();
  console.log('IMPORT TIME', (postImport - preImport) / 1000);

  // const found = await streamToArray(store.match(null, null, null, null));
  //
  // console.log(found);

  // const quads = [];
  // for (let i = 0; i < 20000; i++) {
  //   quads.push(factory.quad(
  //     factory.namedNode('http://ex.com/s'),
  //     factory.namedNode('http://ex.com/p' + Math.floor(i % 100)),
  //     factory.literal('' + i, 'http://www.w3.org/2001/XMLSchema#integer'),
  //     factory.namedNode('http://ex.com/g' + Math.floor(i % 10000))
  //   ));
  // }
  //
  // const preImport = Date.now();
  // await asynctools.waitForEvent(store.import(new ArrayIterator(quads), { batch: 10 }), 'end');
  // const postImport = Date.now();
  //
  // console.log('IMPORT TIME', (postImport - preImport) / 1000);
  //
  // const count = await store.countEstimate(null, null, [
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
  // const preDelete = Date.now();
  // await asynctools.waitForEvent(store.removeMatches(factory.namedNode('http://ex.com/s')), 'end');
  // const postDelete = Date.now();
  //
  // console.log('DELETE TIME', (postDelete - preDelete) / 1000);
  //
  // const preVacuum = Date.now();
  // await asynctools.waitForEvent(store.vacuum(), 'end');
  // const postVacuum = Date.now();
  //
  // console.log('VACUUM TIME', (postVacuum - preVacuum) / 1000);

  //
  // // const preRemove = Date.now();
  // // await asynctools.waitForEvent(store.remove(new ArrayIterator(quads)), 'end');
  // // const postRemove = Date.now();
  // //
  // // console.log('REMOVE TIME', (postRemove - preRemove) / 1000);
  //

  await store.close();

})();

