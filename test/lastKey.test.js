const { DynamoDbStoreWrapper } = require("../index");

describe('lastKey handling test', () => {

    it('test conversions', async () => {

        const expected = {
            index_sort: { S: '0' },
            id: { S: '4e53e6f3-830a-4b8e-8df0-ed1fdcb9d70f' },
            ts: { N: '0' }
          }

        expect(expected).toEqual(DynamoDbStoreWrapper.str2key(DynamoDbStoreWrapper.key2str(expected)));
    });

})

