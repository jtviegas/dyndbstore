[![Build Status](https://travis-ci.org/jtviegas/dyndbstore.svg?branch=master)](https://travis-ci.org/jtviegas/dyndbstore)
[![Coverage Status](https://coveralls.io/repos/github/jtviegas/dyndbstore/badge.svg?branch=master)](https://coveralls.io/github/jtviegas/dyndbstore?branch=master)

dyndbstore
=========

store facade to a database, currently only DynamoDb implementation

## Installation

  `npm install @jtviegas/dyndbstore`

## Usage

### required environment variables or configuration properties

    - DYNDBSTORE_AWS_REGION
    - DYNDBSTORE_AWS_DB_ENDPOINT
    - DYNDBSTORE_AWS_ACCESS_KEY_ID
    - DYNDBSTORE_AWS_ACCESS_KEY

### code snippet example

    var store = require('@jtviegas/dyndbstore');
    // synchronous call this one
    let config = {
                    DYNDBSTORE_AWS_REGION: 'eu-west-1'
                    , DYNDBSTORE_AWS_DB_ENDPOINT: "http://localhost:8000"
                    , DYNDBSTORE_AWS_ACCESS_KEY_ID: process.env.ACCESS_KEY_ID
                    , DYNDBSTORE_AWS_ACCESS_KEY: process.env.ACCESS_KEY
            };
    store.init(config);
    store.getObjsCount(table, (e, r) => {
                if(e)
                    done(e);
                else {
                    expect(r).to.equal(0);
                    done(null);
                }
            });
    
## Tests

  `npm test`

## Contributing

    just help yourself and submit a pull request
