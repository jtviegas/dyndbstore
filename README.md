[![Build Status](https://travis-ci.org/jtviegas/dyndbstore.svg?branch=master)](https://travis-ci.org/jtviegas/dyndbstore)
[![Coverage Status](https://coveralls.io/repos/github/jtviegas/dyndbstore/badge.svg?branch=master)](https://coveralls.io/github/jtviegas/dyndbstore?branch=master)

dyndbstore
=========

store facade to DynamoDb

## Installation

  `npm install @jtviegas/dyndbstore`

## Usage

### environment variables and/or configuration properties

    - DYNDBSTORE_API_VERSION
    - DYNDBSTORE_REGION
    - DYNDBSTORE_ENDPOINT
    - DYNDBSTORE_ACCESS_KEY_ID
    - DYNDBSTORE_ACCESS_KEY

### code snippet example

    var store = require('@jtviegas/dyndbstore');
    // synchronous call this one
    let config = {
                    DYNDBSTORE_API_VERSION: '2012-08-10'
                    , DYNDBSTORE_REGION: 'eu-west-1'
                    , DYNDBSTORE_ENDPOINT: "http://localhost:8000"
                    , DYNDBSTORE_ACCESS_KEY_ID: process.env.ACCESS_KEY_ID
                    , DYNDBSTORE_ACCESS_KEY: process.env.ACCESS_KEY
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

https://github.com/jtviegas/dyndbstore
