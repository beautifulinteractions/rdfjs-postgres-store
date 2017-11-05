
# RDF/JS-POSTGRES-STORE

An implementation of the [RDF/JS Store][1] interface backed by [PostgreSQL][2].

## Usage

    $ npm i beautifulinteractions/rdfjs-postgres-store 
    
&nbsp;

    const Store = require('rdfjs-postgres-store');
    const store = new Store('postgres://127.0.0.1/test');
    
    store.createTables() // creates the necessary tables
    
    // store implements the .match(), .import(), .remove(), 
    // .removeMatches() and deleteGraph() methods. See the interface specs.
    
[1]: https://github.com/rdfjs/representation-task-force/blob/master/interface-spec.md
[2]: https://www.postgresql.org
