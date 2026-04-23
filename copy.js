const AWS = require("aws-sdk");
const winston = require("winston");

// ---------- CONFIGURATION ----------
const REGION = process.env.AWS_REGION || "us-east-1";
const BATCH_SIZE = 500;       // DynamoDB batch write size
const TABLE_PARALLELISM = 5;  // Number of tables copied simultaneously
const SCAN_LIMIT = 1000;      // Maximum items per scan operation to avoid throttling

// ---------- AWS SDK ----------

AWS.config.update({
  region: REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const dynamo = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

// ---------- Railway DynamoDB ----------

const railwayDynamoDB = new AWS.DynamoDB.DocumentClient({
  region: 'us-east-1',  // Replace with Railway's DynamoDB region if different
  endpoint: process.env.RAILWAY_DYNAMODB_URL,  // Railway DynamoDB URL
});

// ---------- LOGGER SETUP ----------
const logger = winston.createLogger({
  level: "info",
  format: winston.format.simple(),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: "copy.log" }),
  ],
});

// ---------- HELPER: Fetch All DynamoDB Tables ----------

async function listAllAWSTables() {
  let tables = [];
  let params = {};
  do {
    try {
      const data = await dynamo.listTables(params).promise();
      tables = tables.concat(data.TableNames);
      params.ExclusiveStartTableName = data.LastEvaluatedTableName;
    } catch (err) {
      logger.error("Error listing DynamoDB tables:", err);
      throw err;
    }
  } while (params.ExclusiveStartTableName);
  return tables;
}

// ---------- HELPER: Fetch Data from AWS DynamoDB with Pagination ----------

async function fetchAWSData(tableName) {
  let params = { TableName: tableName, Limit: SCAN_LIMIT };
  let items = [];

  do {
    try {
      const data = await docClient.scan(params).promise();
      items = items.concat(data.Items);
      params.ExclusiveStartKey = data.LastEvaluatedKey;
    } catch (err) {
      logger.error(`Error fetching data from ${tableName}:`, err);
      throw err;
    }
  } while (params.ExclusiveStartKey);

  return items;
}

// ---------- HELPER: Insert Data into Railway DynamoDB (Copy) ----------

async function copyToRailway(awsData, tableName) {
  for (let i = 0; i < awsData.length; i += BATCH_SIZE) {
    const batch = awsData.slice(i, i + BATCH_SIZE);
    const batchParams = batch.map(item => ({
      PutRequest: {
        Item: item,
      },
    }));

    const params = {
      RequestItems: {
        [tableName]: batchParams,
      },
    };

    try {
      await railwayDynamoDB.batchWrite(params).promise();
      logger.info(`Inserted/updated ${batch.length} items into ${tableName} on Railway`);
    } catch (err) {
      logger.error(`Error inserting data into Railway DynamoDB for ${tableName}:`, err);
      throw err;
    }
  }
}

// ---------- HELPER: Create Tables on Railway DynamoDB (if needed) ----------

async function createTableOnRailway(tableName) {
  const params = {
    TableName: tableName,
    KeySchema: [
      { AttributeName: "customer_id", KeyType: "HASH" },
    ],
    AttributeDefinitions: [
      { AttributeName: "customer_id", AttributeType: "S" },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5,
    },
  };

  try {
    await dynamo.createTable(params).promise();
    logger.info(`Created table: ${tableName} on Railway`);
  } catch (err) {
    logger.error(`Error creating table ${tableName} on Railway:`, err);
    throw err;
  }
}

// ---------- MAIN COPY WORKER ----------

async function copyTable(tableName) {
  logger.info(`Starting copy for table: ${tableName}`);

  // Fetch data from AWS DynamoDB
  const awsData = await fetchAWSData(tableName);

  // Copy data to Railway DynamoDB
  await copyToRailway(awsData, tableName);

  logger.info(`Finished copy for table: ${tableName}`);
}

// ---------- MAIN COPY EXECUTION ----------

async function runCopy() {
  try {
    const tables = await listAllAWSTables();
    logger.info(`Found ${tables.length} tables in DynamoDB`);

    // Create tables on Railway DynamoDB if not already created
    for (const table of tables) {
      await createTableOnRailway(table);  // Create table on Railway if it doesn't exist
    }

    // Process tables in parallel with concurrency limit
    const queue = [...tables];
    const workers = new Array(TABLE_PARALLELISM).fill(null).map(async () => {
      while (queue.length > 0) {
        const table = queue.shift();
        try {
          await copyTable(table);
        } catch (err) {
          logger.error(`Error copying table ${table}:`, err);
        }
      }
    });

    await Promise.all(workers);

    logger.info("All tables copied successfully");
  } catch (err) {
    logger.error("Copy failed:", err);
  }
}

// Start the copy process
runCopy();
