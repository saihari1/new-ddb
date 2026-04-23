const AWS = require("aws-sdk");

// ================= AWS SOURCE (REAL AWS) =================
const aws = new AWS.DynamoDB({
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const docClient = new AWS.DynamoDB.DocumentClient({
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

// ================= RAILWAY TARGET (NO AWS SIGNING) =================
const railway = new AWS.DynamoDB.DocumentClient({
  endpoint: process.env.RAILWAY_DYNAMODB_URL,
  region: process.env.AWS_REGION || "us-east-1",
  convertEmptyValues: true,
});

// Raw client for table operations
const railwayRaw = new AWS.DynamoDB({
  endpoint: process.env.RAILWAY_DYNAMODB_URL,
  region: process.env.AWS_REGION || "us-east-1",
  convertEmptyValues: true,
});

// ================= CHECK TABLE =================
async function tableExists(tableName) {
  try {
    await railwayRaw.describeTable({ TableName: tableName }).promise();
    return true;
  } catch {
    return false;
  }
}

// ================= CREATE TABLE =================
async function createTableFromAWS(tableName) {
  const table = await aws.describeTable({ TableName: tableName }).promise();
  const schema = table.Table;

  await railwayRaw.createTable({
    TableName: schema.TableName,
    KeySchema: schema.KeySchema,
    AttributeDefinitions: schema.AttributeDefinitions,
    ProvisionedThroughput: {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5,
    },
  }).promise();

  console.log("🆕 Created:", tableName);
}

// ================= SCAN AWS =================
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

// ================= COPY TO RAILWAY =================
async function copyData(tableName, items) {
  const BATCH = 25;

  for (let i = 0; i < items.length; i += BATCH) {
    const batch = items.slice(i, i + BATCH);

    await railway.batchWrite({
      RequestItems: {
        [tableName]: batch.map(item => ({
          PutRequest: { Item: item }
        }))
      }
    }).promise();
  }
}

// ================= MAIN =================
async function run() {
  const tables = await aws.listTables().promise();

  for (const tableName of tables.TableNames) {
    console.log("\n🚀 Processing:", tableName);

    const exists = await tableExists(tableName);

    if (!exists) {
      console.log("⚠️ Creating table in Railway...");
      await createTableFromAWS(tableName);
    }

    const items = await scanTable(tableName);

    console.log("📦 Items:", items.length);

    await copyData(tableName, items);

    console.log("✅ Done:", tableName);
  }

  console.log("\n🎉 FULL COPY COMPLETED");
}

run();
