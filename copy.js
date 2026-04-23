const AWS = require("aws-sdk");
const winston = require("winston");

// ================= CONFIG =================
const REGION = process.env.AWS_REGION || "us-east-1";
const BATCH_SIZE = 25; // DynamoDB limit
const TABLE_PARALLELISM = 3;

// ================= AWS SOURCE (REAL DYNAMODB) =================
AWS.config.update({
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const dynamo = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

// ================= RAILWAY TARGET DYNAMODB =================
const railwayDB = new AWS.DynamoDB.DocumentClient({
  endpoint: process.env.RAILWAY_DYNAMODB_URL,
  region: process.env.AWS_REGION || "us-east-1",
  accessKeyId: "dummy",
  secretAccessKey: "dummy",
  sslEnabled: false,
});

// ================= LOGGER =================
const logger = winston.createLogger({
  level: "info",
  format: winston.format.simple(),
  transports: [new winston.transports.Console()],
});

// ================= LIST TABLES =================
async function getTables() {
  let tables = [];
  let params = {};

  do {
    const data = await dynamo.listTables(params).promise();
    tables = tables.concat(data.TableNames);
    params.ExclusiveStartTableName = data.LastEvaluatedTableName;
  } while (params.ExclusiveStartTableName);

  return tables;
}

// ================= SCAN TABLE =================
async function scanTable(tableName) {
  let items = [];
  let params = { TableName: tableName };

  do {
    const data = await docClient.scan(params).promise();
    items = items.concat(data.Items);
    params.ExclusiveStartKey = data.LastEvaluatedKey;
  } while (params.ExclusiveStartKey);

  return items;
}

// ================= WRITE TO RAILWAY =================
async function writeToRailway(tableName, items) {
  for (let i = 0; i < items.length; i += BATCH_SIZE) {
    const batch = items.slice(i, i + BATCH_SIZE);

    const params = {
      RequestItems: {
        [tableName]: batch.map(item => ({
          PutRequest: {
            Item: item,
          },
        })),
      },
    };

    await railwayDB.batchWrite(params).promise();
    logger.info(`✔ Copied ${batch.length} items → ${tableName}`);
  }
}

// ================= PROCESS TABLE =================
async function processTable(tableName) {
  logger.info(`🚀 Processing ${tableName}`);

  const items = await scanTable(tableName);
  logger.info(`Fetched ${items.length} items from ${tableName}`);

  await writeToRailway(tableName, items);

  logger.info(`✅ Done ${tableName}`);
}

// ================= MAIN =================
async function run() {
  try {
    const tables = await getTables();
    logger.info(`Found ${tables.length} tables`);

    const queue = [...tables];

    const workers = new Array(TABLE_PARALLELISM).fill(null).map(async () => {
      while (queue.length > 0) {
        const table = queue.shift();
        try {
          await processTable(table);
        } catch (err) {
          logger.error(`❌ Error ${table}:`, err.message);
        }
      }
    });

    await Promise.all(workers);

    logger.info("🎉 Migration completed successfully");
  } catch (err) {
    logger.error("💥 Migration failed:", err);
  }
}

run();
