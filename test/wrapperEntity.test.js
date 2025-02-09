const { DynamoDbStoreWrapper, AbstractSchema, SimpleItemEntity } = require("../index");
const { setTimeout } = require('timers/promises');
const { faker } = require('@faker-js/faker');
const { v4 } = require('uuid');


describe('DynamoDbStoreWrapper with SimpleItemEntity tests', () => {

    const wrapper = new DynamoDbStoreWrapper({"scanLimit": 3, endpoint: process.env.DYNDBSTORE_TEST_ENDPOINT}, 
        {"*": new SimpleItemEntity()});
    const ID = v4();
    const TS = (new Date()).getTime();
    let table = null
    const OBJ = {
        "id": ID,
        "name": faker.vehicle.model(),
        "description": faker.string.alpha({ length: { min: 5, max: 20 } }) ,
        "price": faker.number.int({ max: 100 }),
        "added": TS,
        "category": faker.animal.bear(),
        "subCategory": faker.animal.bird(),
        "images": [
            {"id": faker.string.uuid(), "name": faker.vehicle.bicycle(), "src": faker.image.dataUri({ type: 'svg-base64', height: 20, width: 20 })}
            , {"id": faker.string.uuid(), "name": faker.vehicle.bicycle(), "src": faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })}
        ]
    }

    beforeEach(async () => {
        table = `test_wrapper_with_simple_entity_${(new Date()).getTime()}`
        await wrapper.createTable(table);
        let status = 'CREATING'
        while( 'CREATING' === status ){         
            await setTimeout(5000)
            status = await wrapper.getTableStatus(table);
        }
        await setTimeout(10000)
    }, 50000);

    it('should put and get the same item', async () => {
        await wrapper.putObj(table, OBJ);
        const result = await wrapper.getObj(table, ID);
        expect(result).toEqual(OBJ);
    }, 30000);

})

