const { v4 } = require('uuid');
const { 
    DynamoDBClient, DeleteTableCommand, ListTablesCommand, CreateTableCommand, QueryCommand,
    PutItemCommand, GetItemCommand, ScanCommand, DeleteItemCommand, DescribeTableCommand
} = require("@aws-sdk/client-dynamodb");

class StoreException extends Error {
    constructor(message) {
        super(message);
    }
}

class NoEntityStoreException extends StoreException {
    constructor(message) {
        super(message);
    }
}

class DynamoDbStore {

    constructor(config) {
        this.client = new DynamoDBClient(config);
        this.scanLimit = config.scanLimit || 12 
    }

    async dropTable(table) {
        console.info("[DynamoDbStore|dropTable|in] (%s)", table);
        const input = { TableName: table};
        const command = new DeleteTableCommand(input);
        const response = await this.client.send(command);
        console.info("[DynamoDbStore|dropTable] response: %o", response);
        console.info("[DynamoDbStore|dropTable|out]");
    };

    async isTable(table) {
        console.info("[DynamoDbStore|isTable|in] (%s)", table);
        const command = new ListTablesCommand({});
        const response = await this.client.send(command);
        console.info("[DynamoDbStore|isTable] response: %o", response)
        const result = 'TableNames' in response ? response["TableNames"].includes(table) : false;
        console.info("[DynamoDbStore|isTable|out] => %s", result);
        return result
    };

    async getTableStatus(name) {
        console.info("[DynamoDbStore|getTableStatus|in] (%s)", name);
        const input = {
            "TableName": name
          };
        const command = new DescribeTableCommand(input);
        const response = await this.client.send(command);
        console.info("[DynamoDbStore|getTableStatus] response: %o", response)
        const result = response["Table"]["TableStatus"]
        console.info("[DynamoDbStore|getTableStatus|out] => %s", result);
        return result
    };

    async createTable(table) {
        console.info("[DynamoDbStore|createTable|in] (%s)", table);

        const input = {
            AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }, { AttributeName: 'ts', AttributeType: 'N' }, { AttributeName: 'index_sort', AttributeType: 'S' }],
            TableName: table,
            KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
            GlobalSecondaryIndexes: [
                {
                    IndexName: "INDEX_SORT",
                    KeySchema: [ { AttributeName: 'index_sort', KeyType: 'HASH' }, { AttributeName: 'ts', KeyType: 'RANGE' } ],
                    Projection: {
                        ProjectionType: "ALL",
                      }
                }
            ],
            BillingMode: "PAY_PER_REQUEST",
            DeletionProtectionEnabled: false,
            SSESpecification: {
                Enabled: false,
            },
            StreamSpecification: {
                StreamEnabled: false
            },
            TableClass: "STANDARD"
        };
        const command = new CreateTableCommand(input);
        const response = await this.client.send(command);
        console.info("[DynamoDbStore|createTable] response: %o", response);

        console.info("[DynamoDbStore|createTable|out]");
    };

    async postObj (table, obj) {
        console.info("[DynamoDbStore|postObj|in] (%s, %o)", table, obj);

        obj.index_sort || (obj.index_sort = {"S": "0"})
        obj.id || (obj.id = {"S": v4()})

        const result = await this.putObj(table, obj);
        delete result.index_sort;
        console.info("[DynamoDbStore|postObj|out] (%o)", result)
        return result
    };

    async putObj (table, obj) {
        console.info("[DynamoDbStore|putObj|in] (%s, %o)", table, obj);
        
        obj.index_sort || (obj.index_sort={"S": "0"})
        const input= { 
            "Item": obj,
            "TableName": table, 
            "ReturnConsumedCapacity": "TOTAL",
          };
        const command = new PutItemCommand(input);
        const response = await this.client.send(command);
        console.info("[DynamoDbStore|putObj] (%o)", response)
        if (0 === response["ConsumedCapacity"]["CapacityUnits"]){
            throw new Error("[DynamoDbStore|putObj|in] no capacity consumed in the operation");
        }
        delete obj.index_sort;
        const result = obj;
        console.info("[DynamoDbStore|putObj|out] (%o)", result)
        return result
    };

    async getObj (table, key) {
        console.info("[DynamoDbStore|getObj|in] (%s, %o)", table, key);
        const input= { 
            "TableName": table, 
            "Key": key
          };
        const command = new GetItemCommand(input);
        const response = await this.client.send(command);
        if (!Object.hasOwn(response, "Item")){
            throw new NoEntityStoreException(`[DynamoDbStore|getObj] no entity with key: ${key}`)
        }
        delete response.Item.index_sort;
        console.info("[DynamoDbStore|getObj|out] => %o", response);
        return response["Item"]
    };

    async getObjs(table, lastKey, desc=true) {
        console.info("[DynamoDbStore|getObjs|in] (%s, %s, %s)", table, lastKey, desc);
        const input= { 
            "TableName": table,
            "Limit": this.scanLimit,
            "IndexName": "INDEX_SORT", 
            "ExpressionAttributeValues": {":a": {"S": "0"}},
            "KeyConditionExpression": "index_sort = :a",
            "ScanIndexForward": !desc,
            "ExclusiveStartKey": lastKey || undefined
        };
        console.info("[DynamoDbStore|getObjs] input: %O", input);
        const command = new QueryCommand(input);
        const response = await this.client.send(command);
        response.Items.forEach((o) => {
            delete o.index_sort;
        })

        const result = { 
            "items": response.Items,
            "lastKey": response.LastEvaluatedKey || undefined
         }

        console.info("[DynamoDbStore|getObjs|out] => %o", result);
        return result
    };

    async delObj (table, key) {
        console.info("[DynamoDbStore|delObj|in] (%s, %o)", table, key);
        const input= { 
            "TableName": table, 
            "Key": key
          };
        const command = new DeleteItemCommand(input);
        const response = await this.client.send(command);
        console.info("[DynamoDbStore|delObj|out] => %o", response);
        return response
    };

    async getAttributeProjection (table, attribute, filter) {
        console.info("[DynamoDbStore|getAttributeProjection|in] (%s, %s, %s)", table, attribute, filter);
        const result = []
        let filterExpression = undefined;
        let expressionAttrValues = undefined;
        if (filter){
            const attribute = Reflect.ownKeys(filter)[0];
            const filterValue = filter[attribute];
            filterExpression = `${attribute} = :a`;
            expressionAttrValues = {
                ":a": filterValue
            }
        }

        const input= { 
            "TableName": table,
            "ProjectionExpression": attribute,
            "FilterExpression": filterExpression,
            "ExpressionAttributeValues": expressionAttrValues
        };

        let lastEvaluatedKey = "";
        while(lastEvaluatedKey !== undefined){
            const command = new ScanCommand(input);
            console.info("[DynamoDbStore|getAttributeProjection] scanning with lastKey: %O", lastEvaluatedKey);
            const response = await this.client.send(command);
            console.info("[DynamoDbStore|getAttributeProjection] response: %o", response);
            lastEvaluatedKey = response.LastEvaluatedKey;

            result.push(...response.Items);
            if(lastEvaluatedKey !== undefined){
                input.ExclusiveStartKey = lastEvaluatedKey;
            } 
        }
        console.info("[DynamoDbStore|getAttributeProjection|out] => %o", result);
        return result
    };

    mapSubAttributes(items, attribute, subAttribute){
        console.info("[DynamoDbStore|mapSubAttributes|in] (%o, %s, %s)", items, attribute, subAttribute);
        const mapping = {};
        for(const item of items){
            const attKey = JSON.stringify(item[attribute])
            if (! Object.hasOwn(mapping, attKey)){
                mapping[attKey] = new Set()
            }
            if(Object.hasOwn(item, subAttribute)){
                (mapping[attKey]).add(item[subAttribute])
            }
        }
        console.info("[DynamoDbStore|mapSubAttributes] mapping: %o", mapping);
        const result = []
        for(const key in mapping){
            const entry = {}
            entry[attribute] = JSON.parse(key)
            entry[subAttribute] = [...mapping[key]]
            result.push(entry)
        }
        console.info("[DynamoDbStore|mapSubAttributes|out] => %o", result);
        return result
    }

    async getSubAttributeMap (table, attribute, subAttribute) {
        console.info("[DynamoDbStore|getSubAttributeMap|in] (%s, %s, %s)", table, attribute, subAttribute);
        const items = []

        const input= { 
            "TableName": table,
            "ProjectionExpression": `${attribute}, ${subAttribute}`
        };

        let lastEvaluatedKey = "";
        while(lastEvaluatedKey !== undefined){
            const command = new ScanCommand(input);
            console.info("[DynamoDbStore|getSubAttributeMap] scanning with lastKey: %O", lastEvaluatedKey);
            const response = await this.client.send(command);
            console.info("[DynamoDbStore|getSubAttributeMap] response: %o", response);
            lastEvaluatedKey = response.LastEvaluatedKey;

            items.push(...response.Items);
            if(lastEvaluatedKey !== undefined){
                input.ExclusiveStartKey = lastEvaluatedKey;
            } 
        }
        const result = this.mapSubAttributes(items, attribute, subAttribute)
        console.info("[DynamoDbStore|getSubAttributeMap|out] => %o", result);
        return result
    };
}

class AbstractSchema {
    getAttributteType(attribute){
        throw new Error("getAttributteType() must be implemented by subclasses");
    }
    toEntity(obj){
        throw new Error("toEntity() must be implemented by subclasses");
    }
    fromEntity(entity){
        throw new Error("fromEntity() must be implemented by subclasses");
    }
}

class SimpleItemEntity extends AbstractSchema {
    
    constructor() {
        super();
        this.types = {
            "id": "S",
            "name": "S",
            "description": "S",
            "price": "N",
            "ts": "N",
            "category": "S",
            "subCategory": "S",
            "images": "L"
        };
    }
    
    getAttributteType(attribute){
        return this.types[attribute]
    }

    toEntity(obj){
        const result =  {
            "name": {"S": obj.name},
            "description": {"S": obj.description},
            "price": {"N": obj.price.toString()},
            "ts": {"N": obj.ts.toString()},
            "category": {"S": obj.category},
            "subCategory": {"S": obj.subCategory},
            "images": {"L": []}
        }
        if(obj.id){
            result.id = {"S": obj.id}
        }
        for(const img of obj.images){
            result.images.L.push(
                {"M": {
                    "id": {"S": img.id},
                    "name": {"S": img.name},
                    "src": {"S": img.src}
                }}
            )
        }
        return result;
    }

    fromEntity(entity){
        const result =  {
            "id": entity.id.S,
            "name": entity.name.S,
            "description": entity.description.S,
            "price": parseInt(entity.price.N),
            "ts": parseInt(entity.ts.N),
            "category": entity.category.S,
            "subCategory": entity.subCategory.S,
            "images": []
        }
        for(const img of entity.images.L){
            result.images.push(
                {
                    "id": img.M.id.S,
                    "name": img.M.name.S,
                    "src": img.M.src.S
                }
            )
        }
        return result;
    }
}

class DynamoDbStoreWrapper {

    static key2str(key) {
        return `${key.id.S}#${key.ts.N}`;
    }

    static str2key(key) {
        const [id, ts] = key.split('#');
        return {
            index_sort: { S: '0' },
            id: { S: id },
            ts: { N: ts }
        };
    }

    constructor(config, schemas) {
        this.store = new DynamoDbStore(config);
        this.schemas = schemas;
    }

    getSchema(table){
        
        let result = undefined;
        if (Object.hasOwn(this.schemas, table)){
            result = this.schemas[table]
        }
        else if (Object.hasOwn(this.schemas, "*")){
            result = this.schemas["*"]
        } 
        else {
            throw new Error(`[getSchema] no schema for table: ${table}`);
        }
        return result;
    }

    getTableSuffix(table){
        const parts = table.split(".")
        return parts[(parts.length - 1) ] 
    }

    async getTableStatus(name) {
        console.info("[DynamoDbStoreWrapper|getTableStatus|in] (%s)", name);
        const result = await this.store.getTableStatus(name);
        console.info("[DynamoDbStoreWrapper|getTableStatus|out] => %s", result);
        return result
    }

    async createTable(name) {
        console.info("[DynamoDbStoreWrapper|createTable|in] (%s)", name);
        await this.store.createTable(name);
        console.info("[DynamoDbStoreWrapper|createTable|out]");
    }

    async dropTable(name) {
        console.info("[DynamoDbStoreWrapper|dropTable|in] (%s)", name);
        await this.store.dropTable(name);
        console.info("[DynamoDbStoreWrapper|dropTable|out]");
    }

    async isTable(name) {
        console.info("[DynamoDbStoreWrapper|isTable|in] (%s)", name);
        const result = await this.store.isTable(name);
        console.info("[DynamoDbStoreWrapper|isTable|out] => %s", result);
        return result
    }

    async postObj (table, obj) {
        console.info("[DynamoDbStoreWrapper|postObj|in] (%s, %o)", table, obj);
        const schema = this.getSchema(this.getTableSuffix(table))
        const response = await this.store.postObj(table, schema.toEntity(obj));
        const result = schema.fromEntity(response);
        console.info("[DynamoDbStoreWrapper|postObj|out] (%o)", result)
        return result
    };

    async putObj (table, obj) {
        console.info("[DynamoDbStoreWrapper|putObj|in] (%s, %o)", table, obj);
        const schema = this.getSchema(this.getTableSuffix(table))
        const response = await this.store.putObj(table, schema.toEntity(obj));
        const result = schema.fromEntity(response);
        console.info("[DynamoDbStoreWrapper|putObj|out] (%o)", result)
        return result
    };

    async getObj (table, id) {
        console.info("[DynamoDbStoreWrapper|getObj|in] (%s, %s)", table, id);
        const schema = this.getSchema(this.getTableSuffix(table))
        const response = await this.store.getObj(table, {"id": {"S": id}});
        const result = schema.fromEntity(response)
        console.info("[DynamoDbStoreWrapper|getObj|out] => %o", result);
        return result
    };

    async getObjs (table, lastKey) {
        console.info("[DynamoDbStoreWrapper|getObjs|in] (%s, %s)", table, lastKey);
        const schema = this.getSchema(this.getTableSuffix(table))
        const lastKeyArg = lastKey ? DynamoDbStoreWrapper.str2key(lastKey) : undefined;
        const response = await this.store.getObjs(table, lastKeyArg);
        const result = {
            "lastKey": response.lastKey ? DynamoDbStoreWrapper.key2str(response.lastKey) : undefined,
            "items": []
        }
        for(const item of response.items){
            result.items.push(schema.fromEntity(item))
        }
        console.info("[DynamoDbStoreWrapper|getObjs|out] => %o", result);
        return result
    };

    async delObj (table, id) {
        console.info("[DynamoDbStoreWrapper|delObj|in] (%s, %s)", table, id);
        const response = await this.store.delObj(table, {"id": {"S": id}});
        console.info("[DynamoDbStoreWrapper|delObj|out] => %o", response);
    };

    async getAttributeProjection (table, attribute, filter) {
        console.info("[DynamoDbStoreWrapper|getAttributeProjection|in] (%s, %s, %s)", table, attribute, filter);
        const result = []
        const schema = this.getSchema(this.getTableSuffix(table));
        let filterWrapper = undefined
        if(filter){
            const filterAttribute = Reflect.ownKeys(filter)[0];
            const filterAttributeType = schema.getAttributteType(filterAttribute);
            filterWrapper = {}
            filterWrapper[filterAttribute] = {}
            filterWrapper[filterAttribute][filterAttributeType] = filter[filterAttribute]
        }
        const response = await this.store.getAttributeProjection(table, attribute, filterWrapper);
        const attrType = schema.getAttributteType(attribute);
        for(const entry of response){
            result.push(entry[attribute][attrType]);
        }
        console.info("[DynamoDbStoreWrapper|getAttributeProjection|out] => %o", result);
        return result
    };

    async getSubAttributeMap (table, attribute, subAttribute) {
        console.info("[DynamoDbStoreWrapper|getSubAttributeMap|in] (%s, %s, %s)", table, attribute, subAttribute);
        const result = {}
        const schema = this.getSchema(this.getTableSuffix(table));
        
        const response = await this.store.getSubAttributeMap(table, attribute, subAttribute);
        const attrType = schema.getAttributteType(attribute);
        const subAttrType = schema.getAttributteType(subAttribute);
        for(const entry of response){
            const attrValue = entry[attribute][attrType]
            const subAttrValues = []
            for( const subAtt of entry[subAttribute] ){
                subAttrValues.push(subAtt[subAttrType])
            }
            result[attrValue] = subAttrValues;
        }
        console.info("[DynamoDbStoreWrapper|getSubAttributeMap|out] => %o", result);
        return result
    };

}

module.exports = {};
module.exports.DynamoDbStore = DynamoDbStore;
module.exports.DynamoDbStoreWrapper = DynamoDbStoreWrapper;
module.exports.AbstractSchema = AbstractSchema;
module.exports.SimpleItemEntity= SimpleItemEntity;
