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

// ================= RAILWAY TARGET =================
const endpoint = process.env.RAILWAY_DYNAMODB_URL;

const railway = new AWS.DynamoDB.DocumentClient({
  endpoint,
  region: "us-east-1",
  accessKeyId: "dummy",
  secretAccessKey: "dummy",
  convertEmptyValues: true,
});

const railwayRaw = new AWS.DynamoDB({
  endpoint,
  region: "us-east-1",
  accessKeyId: "dummy",
  secretAccessKey: "dummy",
});

// ================= WAIT FOR TABLE ACTIVE =================
async function waitForTable(tableName) {
  while (true) {
    const res = await railwayRaw.describeTable({ TableName: tableName }).promise();

    if (res.Table.TableStatus === "ACTIVE") {
      console.log("✅ Table ACTIVE:", tableName);
      return;
    }

    console.log("⏳ Waiting for table:", tableName);
    await new Promise(r => setTimeout(r, 2000));
  }
}

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

// ================= COPY WITH RETRY =================
async function copyData(tableName, items) {
  const BATCH = 25;

  for (let i = 0; i < items.length; i += BATCH) {
    const batch = items.slice(i, i + BATCH);

    const params = {
      RequestItems: {
        [tableName]: batch.map(item => ({
          PutRequest: { Item: item }
        }))
      }
    };

    const res = await railway.batchWrite(params).promise();

    // 🔥 Retry failed writes
    if (res.UnprocessedItems && Object.keys(res.UnprocessedItems).length > 0) {
      console.log("⚠️ Retrying failed items...");
      await railway.batchWrite({ RequestItems: res.UnprocessedItems }).promise();
    }
  }
}

// ================= VERIFY DATA =================
async function verifyTable(tableName) {
  const res = await railway.scan({ TableName: tableName }).promise();
  console.log(`✔ Verified ${tableName}: ${res.Items.length} items`);
}

// ================= MAIN =================
async function run() {
  console.log("🚀 Starting Migration...");
  console.log("🔌 Railway Endpoint:", endpoint);

  const tables = await aws.listTables().promise();

  for (const tableName of tables.TableNames) {
    console.log("\n==============================");
    console.log("🚀 Processing:", tableName);

    const exists = await tableExists(tableName);

    if (!exists) {
      console.log("⚠️ Creating table...");
      await createTableFromAWS(tableName);
      await waitForTable(tableName);
    }

    const items = await scanTable(tableName);

    console.log("📦 Items from AWS:", items.length);

    await copyData(tableName, items);

    await verifyTable(tableName);

    console.log("✅ Done:", tableName);
  }

  console.log("\n🎉 FULL COPY COMPLETED SUCCESSFULLY");
}

run();
