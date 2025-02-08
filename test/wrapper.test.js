const { DynamoDbStoreWrapper, AbstractSchema } = require("../index");
const { setTimeout } = require('timers/promises');
const { faker } = require('@faker-js/faker');
const { v4 } = require('uuid');


class TestEntity extends AbstractSchema {

    toEntity(obj){
        return {
            "id": {"S": obj.id},
            "added": {"N": obj.added.toString()},
            "name": {"S": obj.name},
            "images": {"SS": obj.images}
        }
    }

    fromEntity(entity){
        return {
            "id": entity.id.S,
            "added": parseInt(entity.added.N),
            "name": entity.name.S,
            "images": entity.images.SS
        }
    }
}

describe('DynamoDbStoreWrapper tests', () => {

    const wrapper = new DynamoDbStoreWrapper({"scanLimit": 3, endpoint: process.env.DYNDBSTORE_TEST_ENDPOINT}, 
        {"*": new TestEntity()});
    const ID = v4();
    const TS = (new Date()).getTime();
    let table = null

    beforeEach(async () => {
        table = `test_wrapper_${(new Date()).getTime()}`
        await wrapper.createTable(table);
        let status = 'CREATING'
        while( 'CREATING' === status ){         
            await setTimeout(5000)
            status = await wrapper.getTableStatus(table);
        }
        await setTimeout(10000)
    }, 50000);

    it('should check creation and deletion of table', async () => {
        let isTableResult = await wrapper.isTable(table);
        expect(isTableResult).toEqual(true);
        await wrapper.dropTable(table);
        isTableResult = await wrapper.isTable(table);
        expect(isTableResult).toEqual(false);
    }, 30000);

    it('should put an item', async () => {
        const obj = {
            "id": ID,
            "added": TS,
            "name": faker.vehicle.model(),
            "images": [ "...", "ndbsjkd"]
        }
        const result = await wrapper.putObj(table, obj);
        expect(result).toEqual(obj);
    }, 30000);

    it('should get a specific item', async () => {
        const obj = {
            "id": ID,
            "added": TS,
            "name": faker.vehicle.model(),
            "images": [ "...", "ndbsjkd"]
        }
        await wrapper.putObj(table, obj);
        await wrapper.putObj(table, {
                        "id": faker.string.uuid(),
                        "added": TS,
                        "name": faker.vehicle.model(),
                        "images": [ ".sdasd..", "ndasadsadbsjkd"]
                    });
        await setTimeout(10000)
        const result = await wrapper.getObj(table, ID);
        expect(result).toEqual(obj);
    }, 30000);

    it('should get all items', async () => {
        const items = []
        for(let i=0; i<3; i++){
            let item = {
                "id": faker.string.uuid(),
                "added": TS,
                "name": faker.vehicle.model(),
                "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
            }
            await wrapper.putObj(table, item);
            items.push(item);  
        }
        await setTimeout(5000)
        const result = await wrapper.getObjs(table);
        expect(result.items.length).toEqual(3);
        expect(result.items.toSorted((a, b) => a.name.localeCompare(b.name))).toEqual(items.toSorted((a, b) => a.name.localeCompare(b.name)));
    }, 30000);

    it('should update a specific item', async () => {
        const item = {
            "id": ID,
            "added": TS,
            "name": faker.vehicle.model(),
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
        }
        await wrapper.putObj(table, item);
        await setTimeout(5000)
        item.name = "test2"
        await wrapper.putObj(table, item);
        await setTimeout(5000)
        const result = await wrapper.getObj(table, ID);
        expect(result.name).toEqual("test2");
        expect(result).toEqual(item);
    }, 30000);

    it('should delete a specific item', async () => {
        const ID2 = faker.string.uuid();
        const items = [
            {"id": ID,
            "added": TS,
            "name": faker.vehicle.model(),
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
            },
            {"id": ID2,
            "added": TS,
            "name": faker.vehicle.model(),
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
            }
        ];
        for(let item of items){
            await wrapper.putObj(table, item);
        }
        await setTimeout(5000)
        await wrapper.delObj(table, ID);
        await setTimeout(5000)

        const result = await wrapper.getObjs(table);
        expect(result.items.length).toEqual(1);
        expect(result.items[0].id).toEqual(ID2);

    }, 30000);

    it('should paginate items', async () => {
    
        const items = []
        for(let i=0; i<5; i++){
            let item = {
                "id": faker.string.uuid(),
                "added": TS,
                "name": faker.vehicle.model(),
                "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
            }
            await wrapper.putObj(table, item);
            items.push(item);  
        }
        await setTimeout(5000)
        const result = await wrapper.getObjs(table);
        expect(result.items.length).toEqual(3);
        expect(result.lastKey).not.toEqual(undefined);
        const result2 = await wrapper.getObjs(table, result.lastKey);
        expect(result2.items.length).toEqual(2);
        expect(result.items).toEqual(expect.not.arrayContaining(result2.items));
    }, 30000);

})

