
# RDF/JS-POSTGRES-STORE

An implementation of the [RDF/JS Store][1] interface backed by [PostgreSQL][2].

## Status

**HIGHLY EXPERIMENTAL**

## Usage

    $ npm i beautifulinteractions/rdfjs-postgres-store 
    
&nbsp;

    const Store = require('rdfjs-postgres-store');
    const store = new Store('postgres://127.0.0.1/test');
    
    store.createTables() // creates the necessary tables
    
    // store implements the .match(), .import(), .remove(), 
    // .removeMatches() and deleteGraph() methods. See the interface specs.

### Filters

Supports advanced filters:

    const iterator = await store.match(null, null, null, [
        { test: 'gt', comparate: factory.literal('5', 'http://www.w3.org/2001/XMLSchema#integer') },
        { test: 'lt', comparate: factory.literal('10', 'http://www.w3.org/2001/XMLSchema#integer') }
    ], null);
    
### Count

Supports estimated count:

    const count = await store.estimate(null, null, null, [
        { test: 'gt', comparate: factory.literal('5', 'http://www.w3.org/2001/XMLSchema#integer') },
        { test: 'lt', comparate: factory.literal('10', 'http://www.w3.org/2001/XMLSchema#integer') }
    ], null);
    
[1]: https://github.com/rdfjs/representation-task-force/blob/master/interface-spec.md
[2]: https://www.postgresql.org
