dyndbstore
=========

store facade to DynamoDb

## Installation

  `npm install @jtviegas/dyndbstore`

## Usage

    var store = require('@jtviegas/dyndbstore');
    // synchronous call this one
    store.init({ apiVersion: '2012-08-10' , region: 'eu-west-1' , 
        'endpoint': "http://localhost:8000"}); 
        
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
