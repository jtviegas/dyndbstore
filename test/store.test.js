const { DynamoDbStore } = require("../index");
const { setTimeout } = require('timers/promises');
const { faker } = require('@faker-js/faker');
const { v4 } = require('uuid');


describe('DynamoDbStore tests', () => {


    const store = new DynamoDbStore({"scanLimit": 3, endpoint: process.env.DYNDBSTORE_TEST_ENDPOINT});
    const ID = v4();
    const TS = (new Date()).getTime();
    let table = `test_table_${(new Date()).getTime()}`

    beforeEach(async () => {
        table = `test_table_${(new Date()).getTime()}`
        await store.createTable(table);
        await setTimeout(2000)
    }, 30000);

    it('should check creation and deletion of table', async () => {
        let isTableResult = await store.isTable(table);
        expect(isTableResult).toEqual(true);
        await store.dropTable(table);
        await setTimeout(2000)
        isTableResult = await store.isTable(table);
        expect(isTableResult).toEqual(false);
    }, 30000);


    it('should post an item', async () => {
        const item = {
            "ts": {"N": TS.toString()},
            "name": {"S": "test"}
        }
        const result = await store.postObj(table, item);
        expect(result.ts.N).toEqual(item.ts.N);
        expect(result.name.S).toEqual(item.name.S);
    }, 30000);

    it('should put an item', async () => {
        const item = {
            "id": {"S": ID},
            "ts": {"N": TS.toString()},
            "name": {"S": "test"}
        }
        const result = await store.putObj(table, item);
        expect(result).toEqual(item);
    }, 30000);

    it('should get a specific item', async () => {
        const item = {
            "id": {"S": ID},
            "ts": {"N": TS.toString()},
            "name": {"S": "test"}
        }
        await store.putObj(table, item);
        await store.putObj(table, {
                        "id": {"S": faker.string.uuid()},
                        "ts": {"N": TS.toString()},
                        "name": {"S": faker.vehicle.model()}
                    });
        await setTimeout(2000)
        const result = await store.getObj(table, {
            "id": {"S": ID}
        });

        expect(result).toEqual(item);
    }, 30000);

    it('should get all items', async () => {
        const items = []
        for(let i=0; i<3; i++){
            let item = {
                "id": {"S": faker.string.uuid()},
                "ts": {"N": TS.toString()},
                "name": {"S": faker.vehicle.model()}
            }
            await store.putObj(table, item);
            items.push(item);  
        }
        await setTimeout(2000)
        const result = await store.getObjs(table);
        expect(result.items.length).toEqual(3);
        expect(result.items.toSorted((a, b) => a.name.S.localeCompare(b.name.S))).toEqual(items.toSorted((a, b) => a.name.S.localeCompare(b.name.S)));
    }, 30000);

    it('should update a specific item', async () => {
        const item = {
            "id": {"S": ID},
            "ts": {"N": TS.toString()},
            "name": {"S": "test"}
        }
        await store.putObj(table, item);
        await setTimeout(2000)
        item.name.S = "test2"
        await store.putObj(table, item);
        await setTimeout(2000)
        const result = await store.getObj(table, {
            "id": {"S": ID}
        });
        expect(result.name.S).toEqual("test2");
        expect(result).toEqual(item);
    }, 30000);

    it('should delete a specific item', async () => {
        const ID2 = faker.string.uuid();
        const items = [
            {"id": {"S": ID},
            "ts": {"N": TS.toString()},
            "name": {"S": "test"}},
            {"id": {"S": ID2},
                "ts": {"N": TS.toString()},
                "name": {"S": faker.vehicle.model()}}
        ];
        for(let item of items){
            await store.putObj(table, item);
        }
        await setTimeout(2000)
        await store.delObj(table, {"id": {"S": ID}});
        await setTimeout(2000)

        const result = await store.getObjs(table);
        expect(result.items.length).toEqual(1);
        expect(result.items[0].id.S).toEqual(ID2);

    }, 30000);

    it('should paginate items', async () => {

        const items = []
        for(let i=0; i<5; i++){
            let item = {
                "id": {"S": faker.string.uuid()},
                "ts": {"N": TS.toString()},
                "name": {"S": faker.vehicle.model()}
            }
            await store.putObj(table, item);
            items.push(item);  
        }
        await setTimeout(2000)
        const result = await store.getObjs(table);
        expect(result.items.length).toEqual(3);
        expect(result.lastKey).not.toEqual(undefined);
        const result2 = await store.getObjs(table, result.lastKey);
        expect(result2.items.length).toEqual(2);
        expect(result.items).toEqual(expect.not.arrayContaining(result2.items));
    }, 30000);

    it('should get some kind of projection', async () => {

        const items = []
        for(let i=0; i<5; i++){
            let item = {
                "id": {"S": faker.string.uuid()},
                "ts": {"N": TS.toString()},
                "category": {"S": faker.vehicle.model()}
            }
            await store.putObj(table, item);
            items.push(item);  
        }
        await setTimeout(2000)

        const result = await store.getAttributeProjection(table, "category");
        expect(result.length).toEqual(5);
    }, 60000);

    it('should get some kind of projection with a filter', async () => {

        await store.putObj(table, {
            "id": {"S": faker.string.uuid()},
            "ts": {"N": TS.toString()},
            "category": {"S": "xpto"},
            "subCategory": {"S": "AAA"}
        });
        await store.putObj(table, {
            "id": {"S": faker.string.uuid()},
            "ts": {"N": TS.toString()},
            "category": {"S": "xpto"},
            "subCategory": {"S": "BBB"}
        });

        const subcats = [{ subCategory: { S: 'BBB' } },
            { subCategory: { S: 'AAA' } }];

        for(let i=0; i<3; i++){
            let item = {
                "id": {"S": faker.string.uuid()},
                "ts": {"N": TS.toString()},
                "category": {"S": faker.vehicle.model()},
                "subCategory": {"S": faker.food.adjective()}
            }
            await store.putObj(table, item);
        }
        await setTimeout(2000)

        const result = await store.getAttributeProjection(table, "subCategory", {"category": {"S": "xpto"}});
        expect(result.length).toEqual(2);
        expect(subcats).toEqual(expect.arrayContaining(result));
    }, 60000);

    it('should get the subAttribute map', async () => {

        await store.putObj(table, {
            "id": {"S": faker.string.uuid()},
            "ts": {"N": TS.toString()},
            "category": {"S": "xpto"},
            "subCategory": {"S": "AAA"}
        });
        await store.putObj(table, {
            "id": {"S": faker.string.uuid()},
            "ts": {"N": TS.toString()},
            "category": {"S": "xpto"},
            "subCategory": {"S": "BBB"}
        });
        await store.putObj(table, {
            "id": {"S": faker.string.uuid()},
            "ts": {"N": TS.toString()},
            "category": {"S": "abcd"},
            "subCategory": {"S": "BBB"}
        });
        await store.putObj(table, {
            "id": {"S": faker.string.uuid()},
            "ts": {"N": TS.toString()},
            "category": {"S": "abcd"}
        });
        await store.putObj(table, {
            "id": {"S": faker.string.uuid()},
            "ts": {"N": TS.toString()},
            "category": {"S": "xyzw"}
        });

        await setTimeout(2000)
        const expected = [
            {
                category: { S: 'abcd' },
                subCategory: [ { S: 'BBB' } ]
            },
            {
                category: { S: 'xpto' },
                subCategory: [ { S: 'AAA' }, { S: 'BBB' }]
            },
            {
                category: { S: 'xyzw' },
                subCategory: []
            }
          ];
        // sort the result before comparing
        const result = (await store.getSubAttributeMap(table, "category", "subCategory")).toSorted((a, b) => a.category.S.localeCompare(b.category.S));
        result[1]["subCategory"] = (result[1]["subCategory"]).toSorted((a, b) => a.S.localeCompare(b.S))
        expect(result.length).toEqual(3);
        expect(expected).toEqual(expect.arrayContaining(result));
    }, 60000);

});
