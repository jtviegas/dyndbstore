const { DynamoDbStoreWrapper, AbstractSchema } = require("../index");
const { setTimeout } = require('timers/promises');
const { faker } = require('@faker-js/faker');
const { v4 } = require('uuid');


class TestEntity extends AbstractSchema {

    constructor() {
        super();
        this.types = {
            "id": "S",
            "added": "N",
            "category": "S",
            "subCategory": "S",
            "images": "SS"
        };
    }
    
    getAttributteType(attribute){
        return this.types[attribute]
    }

    toEntity(obj){
        const result = {
            "added": {"N": obj.added.toString()},
            "category": {"S": obj.category},
            "subCategory": {"S": obj.subCategory},
            "images": {"SS": obj.images}
        }
        if(obj.id){
            result.id = {"S": obj.id}
        }
        return result
    }

    fromEntity(entity){
        return {
            "id": entity.id.S,
            "added": parseInt(entity.added.N),
            "category": entity.category.S,
            "subCategory": entity.subCategory.S,
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
            "category": faker.vehicle.model(),
            "subCategory": faker.animal.bird(),
            "images": [ "...", "ndbsjkd"]
        }
        const result = await wrapper.putObj(table, obj);
        expect(result).toEqual(obj);
    }, 30000);

    it('should post a new item', async () => {
        const obj = {
            "added": TS,
            "category": faker.vehicle.model(),
            "subCategory": faker.animal.bird(),
            "images": [ "...", "ndbsjkd"]
        }
        const response = await wrapper.postObj(table, obj);
        await setTimeout(10000)
        expect(response.added).toEqual(obj.added);
        expect(response.category).toEqual(obj.category);
        expect(response.subCategory).toEqual(obj.subCategory);
        expect(response.images).toEqual(obj.images);

    }, 30000);

    it('should get a specific item', async () => {
        const obj = {
            "id": ID,
            "added": TS,
            "category": faker.vehicle.model(),
            "subCategory": faker.animal.bird(),
            "images": [ "...", "ndbsjkd"]
        }
        await wrapper.putObj(table, obj);
        await wrapper.putObj(table, {
                        "id": faker.string.uuid(),
                        "added": TS,
                        "category": faker.vehicle.model(),
                        "subCategory": faker.animal.bird(),
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
                "category": faker.vehicle.model(),
                "subCategory": faker.animal.bird(),
                "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
            }
            await wrapper.putObj(table, item);
            items.push(item);  
        }
        await setTimeout(5000)
        const result = await wrapper.getObjs(table);
        expect(result.items.length).toEqual(3);
        expect(result.items.toSorted((a, b) => a.category.localeCompare(b.category))).toEqual(items.toSorted((a, b) => a.category.localeCompare(b.category)));
    }, 30000);

    it('should update a specific item', async () => {
        const item = {
            "id": ID,
            "added": TS,
            "category": faker.vehicle.model(),
            "subCategory": faker.animal.bird(),
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
        }
        await wrapper.putObj(table, item);
        await setTimeout(5000)
        item.category = "test2"
        await wrapper.putObj(table, item);
        await setTimeout(5000)
        const result = await wrapper.getObj(table, ID);
        expect(result.category).toEqual("test2");
        expect(result).toEqual(item);
    }, 30000);

    it('should delete a specific item', async () => {
        const ID2 = faker.string.uuid();
        const items = [
            {"id": ID,
            "added": TS,
            "category": faker.vehicle.model(),
            "subCategory": faker.animal.bird(),
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
            },
            {"id": ID2,
            "added": TS,
            "category": faker.vehicle.model(),
            "subCategory": faker.animal.bird(),
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
                "category": faker.vehicle.model(),
                "subCategory": faker.animal.bird(),
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

    it('should get some kind of projection', async () => {

        const cats = []
        for(let i=0; i<5; i++){
            let item = {
                "id": faker.string.uuid(),
                "added": TS,
                "category": faker.vehicle.model(),
                "subCategory": faker.animal.bird(),
                "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
            }
            await wrapper.putObj(table, item);
            cats.push(item.category);  
        }
        await setTimeout(5000)
        
        const result = await wrapper.getAttributeProjection(table, "category");
        expect(result.length).toEqual(5);
        expect(result.toSorted((a, b) => a.localeCompare(b))).toEqual(cats.toSorted((a, b) => a.localeCompare(b)));

    }, 60000);

    it('should get some kind of projection with a filter', async () => {


        await wrapper.putObj(table, {
            "id": faker.string.uuid(),
            "added": TS,
            "category": "xpto",
            "subCategory": "AAA",
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
        });
        await wrapper.putObj(table, {
            "id": faker.string.uuid(),
            "added": TS,
            "category": "xpto",
            "subCategory": "BBB",
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
        });

        const subcats = ['BBB', 'AAA'];

        for(let i=0; i<3; i++){
            let item = {
                "id": faker.string.uuid(),
                "added": TS,
                "category": faker.vehicle.model(),
                "subCategory": faker.animal.bird(),
                "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
            }
            await wrapper.putObj(table, item);
        }
        await setTimeout(5000)

        const result = await wrapper.getAttributeProjection(table, "subCategory", {"category": "xpto"});
        expect(result.length).toEqual(2);
        expect(subcats).toEqual(expect.arrayContaining(result));
    }, 60000);

    it('should get the subAttribute map', async () => {

        await wrapper.putObj(table, {
            "id": faker.string.uuid(),
            "added": TS,
            "category": "xpto",
            "subCategory": "AAA",
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
        });
        await wrapper.putObj(table, {
            "id": faker.string.uuid(),
            "added": TS,
            "category": "xpto",
            "subCategory":  "BBB",
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
        });
        await wrapper.putObj(table, {
            "id": faker.string.uuid(),
            "added": TS,
            "category": "abcd",
            "subCategory": "BBB",
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
        });
        await wrapper.putObj(table, {
            "id": faker.string.uuid(),
            "added": TS,
            "category": "abcd",
            "subCategory":  "tty",
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
        });
        await wrapper.putObj(table, {
            "id": faker.string.uuid(),
            "added": TS,
            "category": "xyzw",
            "subCategory":  "tty",
            "images": [faker.image.dataUri({ type: 'svg-base64', height: 30, width: 30 })]
        });

        await setTimeout(5000)
        const expected = {'abcd': ['tty', 'BBB'], 'xpto': ['AAA', 'BBB'], 'xyzw': ['tty']};
        // sort the result before comparing
        const result = await wrapper.getSubAttributeMap(table, "category", "subCategory");
        for(key in result){
            subs = result[key]
            result[key] = subs.toSorted((a, b) => a.localeCompare(b));
            expect(result[key]).toEqual(expect.arrayContaining(expected[key]));
        }
        expect(Object.keys(result).length).toEqual(3);
        expect(Object.keys(expected)).toEqual(expect.arrayContaining(Object.keys(result)));
    }, 60000);

})

