const winston = require('winston');
const { 
    DynamoDBClient, DeleteTableCommand, ListTablesCommand, CreateTableCommand,
    PutItemCommand, GetItemCommand, ScanCommand, DeleteItemCommand, DescribeTableCommand
} = require("@aws-sdk/client-dynamodb");

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.splat(),
        winston.format.timestamp(),
        winston.format.printf(info => {
                return `${info.timestamp} ${info.level}: ${info.message}`;
            })
        ),
    transports: [new winston.transports.Console()],
    exitOnError: false
});





class DynamoDbStore {

    constructor(config) {
        this.client = new DynamoDBClient(config);
        this.scanLimit = config.scanLimit || 12 
    }

    async dropTable(table) {
        logger.info("[DynamoDbStore|dropTable|in] (%s)", table);
        const input = { TableName: table};
        const command = new DeleteTableCommand(input);
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|dropTable] response: %o", response);
        logger.info("[DynamoDbStore|dropTable|out]");
    };

    async isTable(table) {
        logger.info("[DynamoDbStore|isTable|in] (%s)", table);
        const command = new ListTablesCommand({});
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|isTable] response: %o", response)
        const result = 'TableNames' in response ? response["TableNames"].includes(table) : false;
        logger.info("[DynamoDbStore|isTable|out] => %s", result);
        return result
    };

    async getTableStatus(name) {
        logger.info("[DynamoDbStore|getTableStatus|in] (%s)", name);
        const input = {
            "TableName": name
          };
        const command = new DescribeTableCommand(input);
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|getTableStatus] response: %o", response)
        const result = response["Table"]["TableStatus"]
        logger.info("[DynamoDbStore|getTableStatus|out] => %s", result);
        return result
    };

    async createTableWithId(table) {
        logger.info("[DynamoDbStore|createTableWithId|in] (%s)", table);

        const input = {
            AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
            TableName: table,
            KeySchema: [ { AttributeName: 'id', KeyType: 'HASH' } ],
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
        logger.info("[DynamoDbStore|createTableWithId] response: %o", response);

        logger.info("[DynamoDbStore|createTableWithId|out]");
    };

    async putObj (table, obj) {
        logger.info("[DynamoDbStore|putObj|in] (%s, %o)", table, obj);
        const input= { 
            "Item": obj,
            "TableName": table, 
            "ReturnConsumedCapacity": "TOTAL",
          };
        const command = new PutItemCommand(input);
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|putObj] (%o)", response)
        const result = response["ConsumedCapacity"]["CapacityUnits"] === 1 ? obj : undefined;
        logger.info("[DynamoDbStore|putObj|out] (%o)", result)
        return result
    };

    async getObj (table, key) {
        logger.info("[DynamoDbStore|getObj|in] (%s, %o)", table, key);
        const input= { 
            "TableName": table, 
            "Key": key
          };
        const command = new GetItemCommand(input);
        const response = await this.client.send(command);

        logger.info("[DynamoDbStore|getObj|out] => %o", response);
        return response["Item"]
    };

    async getObjs (table, key) {
        logger.info("[DynamoDbStore|getObjs|in] (%s, %s)", table, key);
        const input= { 
            "TableName": table,
            "Limit": this.scanLimit
        };
        input.ExclusiveStartKey = key || undefined

        const command = new ScanCommand(input);
        const response = await this.client.send(command);
        const result = { 
            "items": response.Items,
            "lastKey": response.LastEvaluatedKey || undefined
         }
        logger.info("[DynamoDbStore|getObjs|out] => %o", result);
        return result
    };

    async delObj (table, key) {
        logger.info("[DynamoDbStore|delObj|in] (%s, %o)", table, key);
        const input= { 
            "TableName": table, 
            "Key": key
          };
        const command = new DeleteItemCommand(input);
        const response = await this.client.send(command);
        logger.info("[DynamoDbStore|delObj|out] => %o", response);
        return response
    };

    async getAttributeProjection (table, attribute, filter) {
        logger.info("[DynamoDbStore|getAttributeProjection|in] (%s, %s, %s)", table, attribute, filter);
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
            logger.info("[DynamoDbStore|getAttributeProjection] scanning with lastKey: %s", lastEvaluatedKey);
            const response = await this.client.send(command);
            logger.info("[DynamoDbStore|getAttributeProjection] response: %o", response);
            lastEvaluatedKey = response.LastEvaluatedKey;

            result.push(...response.Items);
            if(lastEvaluatedKey !== undefined){
                input.ExclusiveStartKey = lastEvaluatedKey;
            } 
        }
        logger.info("[DynamoDbStore|getAttributeProjection|out] => %o", result);
        return result
    };

    mapSubAttributes(items, attribute, subAttribute){
        logger.info("[DynamoDbStore|mapSubAttributes|in] (%o, %s, %s)", items, attribute, subAttribute);
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
        logger.info("[DynamoDbStore|mapSubAttributes] mapping: %o", mapping);
        const result = []
        for(const key in mapping){
            const entry = {}
            entry[attribute] = JSON.parse(key)
            entry[subAttribute] = [...mapping[key]]
            result.push(entry)
        }
        logger.info("[DynamoDbStore|mapSubAttributes|out] => %o", result);
        return result
    }

    async getSubAttributeMap (table, attribute, subAttribute) {
        logger.info("[DynamoDbStore|getSubAttributeMap|in] (%s, %s, %s)", table, attribute, subAttribute);
        const items = []

        const input= { 
            "TableName": table,
            "ProjectionExpression": `${attribute}, ${subAttribute}`
        };

        let lastEvaluatedKey = "";
        while(lastEvaluatedKey !== undefined){
            const command = new ScanCommand(input);
            logger.info("[DynamoDbStore|getSubAttributeMap] scanning with lastKey: %s", lastEvaluatedKey);
            const response = await this.client.send(command);
            logger.info("[DynamoDbStore|getSubAttributeMap] response: %o", response);
            lastEvaluatedKey = response.LastEvaluatedKey;

            items.push(...response.Items);
            if(lastEvaluatedKey !== undefined){
                input.ExclusiveStartKey = lastEvaluatedKey;
            } 
        }
        const result = this.mapSubAttributes(items, attribute, subAttribute)
        logger.info("[DynamoDbStore|getSubAttributeMap|out] => %o", result);
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
            "added": "N",
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
            "id": {"S": obj.id},
            "name": {"S": obj.name},
            "description": {"S": obj.description},
            "price": {"N": obj.price.toString()},
            "added": {"N": obj.added.toString()},
            "category": {"S": obj.category},
            "subCategory": {"S": obj.subCategory},
            "images": {"L": []}
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
            "added": parseInt(entity.added.N),
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
        logger.info("[DynamoDbStoreWrapper|getTableStatus|in] (%s)", name);
        const result = await this.store.getTableStatus(name);
        logger.info("[DynamoDbStoreWrapper|getTableStatus|out] => %s", result);
        return result
    }

    async createTable(name) {
        logger.info("[DynamoDbStoreWrapper|createTable|in] (%s)", name);
        await this.store.createTableWithId(name);
        logger.info("[DynamoDbStoreWrapper|createTable|out]");
    }

    async dropTable(name) {
        logger.info("[DynamoDbStoreWrapper|dropTable|in] (%s)", name);
        await this.store.dropTable(name);
        logger.info("[DynamoDbStoreWrapper|dropTable|out]");
    }

    async isTable(name) {
        logger.info("[DynamoDbStoreWrapper|isTable|in] (%s)", name);
        const result = await this.store.isTable(name);
        logger.info("[DynamoDbStoreWrapper|isTable|out] => %s", result);
        return result
    }

    async putObj (table, obj) {
        logger.info("[DynamoDbStoreWrapper|putObj|in] (%s, %o)", table, obj);
        const schema = this.getSchema(this.getTableSuffix(table))
        const response = await this.store.putObj(table, schema.toEntity(obj));
        const result = response ? schema.fromEntity(response) : undefined
        logger.info("[DynamoDbStoreWrapper|putObj|out] (%o)", result)
        return result
    };

    async getObj (table, id) {
        logger.info("[DynamoDbStoreWrapper|getObj|in] (%s, %s)", table, id);
        const schema = this.getSchema(this.getTableSuffix(table))
        const response = await this.store.getObj(table, {"id": {"S": id}});
        const result = schema.fromEntity(response)
        logger.info("[DynamoDbStoreWrapper|getObj|out] => %o", result);
        return result
    };

    async getObjs (table, lastKey) {
        logger.info("[DynamoDbStoreWrapper|getObjs|in] (%s, %s)", table, lastKey);
        const schema = this.getSchema(this.getTableSuffix(table))
        const response = await this.store.getObjs(table, lastKey);
        const result = {
            "lastKey": response.lastKey,
            "items": []
        }
        for(const item of response.items){
            result.items.push(schema.fromEntity(item))
        }
        logger.info("[DynamoDbStoreWrapper|getObjs|out] => %o", result);
        return result
    };

    async delObj (table, id) {
        logger.info("[DynamoDbStoreWrapper|delObj|in] (%s, %s)", table, id);
        const response = await this.store.delObj(table, {"id": {"S": id}});
        logger.info("[DynamoDbStoreWrapper|delObj|out] => %o", response);
    };

    async getAttributeProjection (table, attribute, filter) {
        logger.info("[DynamoDbStoreWrapper|getAttributeProjection|in] (%s, %s, %s)", table, attribute, filter);
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
        logger.info("[DynamoDbStoreWrapper|getAttributeProjection|out] => %o", result);
        return result
    };

    async getSubAttributeMap (table, attribute, subAttribute) {
        logger.info("[DynamoDbStoreWrapper|getSubAttributeMap|in] (%s, %s, %s)", table, attribute, subAttribute);
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
        logger.info("[DynamoDbStoreWrapper|getSubAttributeMap|out] => %o", result);
        return result
    };

}

module.exports = {};
module.exports.DynamoDbStore = DynamoDbStore;
module.exports.DynamoDbStoreWrapper = DynamoDbStoreWrapper;
module.exports.AbstractSchema = AbstractSchema;
module.exports.SimpleItemEntity= SimpleItemEntity;
