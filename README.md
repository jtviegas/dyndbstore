![master build & test](https://github.com/jtviegas/dyndbstore/workflows/master%20build%20&%20test/badge.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/jtviegas/dyndbstore/badge.svg?branch=master)](https://coveralls.io/github/jtviegas/dyndbstore?branch=master)

dyndbstore
=========

store facade to a database, currently only DynamoDb implementation

## Installation

  `npm install @jtviegas/dyndbstore`

## Usage

### required environment variables
    
    - region - aws region ( not mandatory, default: eu-west-1 )
    - AWS_ACCESS_KEY_ID ( mandatory )
    - AWS_SECRET_ACCESS_KEY ( mandatory )
    - DYNDBSTORE_TEST_ENDPOINT ( not mandatory, for testing purposes )

### code snippet example

    var store = require('@jtviegas/dyndbstore');
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
