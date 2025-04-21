const { Client } = require("@elastic/elasticsearch");
const { createReadStream } = require("fs");
const split = require("split2");
require("dotenv").config();

const ELASTICSEARCH_ENDPOINT = process.env.ELASTICSEARCH_ENDPOINT;
const ELASTICSEARCH_API_KEY = process.env.ELASTICSEARCH_API_KEY;

const esClient = new Client({
  node: ELASTICSEARCH_ENDPOINT,
  auth: { apiKey: ELASTICSEARCH_API_KEY },
});

const INDEX_NAME = "vet-visits";

const main = async () => {
  try {
    await createMappings(INDEX_NAME, {
      properties: {
        owner_name: {
          type: "text",
        },
        pet_name: {
          type: "text",
        },
        species: {
          type: "keyword",
        },
        breed: {
          type: "keyword",
        },
        vaccination_history: {
          type: "keyword",
        },
        visit_details: {
          type: "text",
        },
      },
    });

    await indexData("./data.ndjson", INDEX_NAME);
  } catch (error) {
    console.error("Error in main function:", error);

    throw error;
  }
};

const createMappings = async (indexName, mapping) => {
  try {
    console.log(`Creating mappings for index ${indexName}...`);

    const body = await esClient.indices.create({
      index: indexName,
      body: {
        mappings: mapping,
      },
    });

    console.log("Index created successfully:", body);
  } catch (error) {
    console.error("Error creating mapping:", error);
  }
};

const indexData = async (filePath, indexName) => {
  try {
    console.log(`Indexing data from ${filePath} into ${indexName}...`);

    const result = await esClient.helpers.bulk({
      datasource: createReadStream(filePath).pipe(split()),

      onDocument: () => {
        return {
          index: { _index: indexName },
        };
      },
      onDrop(doc) {
        console.error("Error processing document:", doc);
      },
    });

    console.log(
      `Bulk indexing completed. Total documents: ${result.successful}, Failed: ${result.failed}`
    );
  } catch (error) {
    console.error("Error indexing data:", error);
    throw error;
  }
};

// Execute the main function
main();
