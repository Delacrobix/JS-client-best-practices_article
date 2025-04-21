const bodyParser = require("body-parser");
const express = require("express");
const { Client, errors } = require("@elastic/elasticsearch");

require("dotenv").config();

const ELASTICSEARCH_ENDPOINT = process.env.ELASTICSEARCH_ENDPOINT;
const ELASTICSEARCH_API_KEY = process.env.ELASTICSEARCH_API_KEY;
const PORT = 3000;

let esClient = new Client({
  node: ELASTICSEARCH_ENDPOINT,
  auth: { apiKey: ELASTICSEARCH_API_KEY },
});

const app = express();

app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});
app.use(bodyParser.json());

app.get("/ping", async (req, res) => {
  try {
    const result = await esClient.info();

    res.status(200).json({
      success: true,
      clusterInfo: result,
    });
  } catch (error) {
    console.error("Error getting Elasticsearch info:", error);

    res.status(500).json({
      success: false,
      clusterInfo: null,
      error: error.message,
    });
  }
});

app.get("/esql/torecords", async (__, res) => {
  const q = `FROM vet-visits 
  | KEEP owner_name, pet_name, species, breed, vaccination_history, visit_details 
  | LIMIT 5`;

  try {
    const results = await esClient.helpers.esql({ query: q }).toRecords();

    return res.status(200).json({
      success: true,
      results: results,
    });
  } catch (error) {
    //TODO: handle errors

    res.status(500).json({
      success: false,
      results: null,
      error: error,
    });
  }
});

app.get("/esql/toarrowreader", async (__, res) => {
  const q = `FROM vet-visits`;

  try {
    const reader = await esClient.helpers.esql({ query: q }).toArrowReader();

    // console.log("Reader:", reader);
    // for (const recordBatch of reader) {
    //   for (const record of recordBatch) {
    //     console.log(record.toJSON());
    //   }
    // }

    return res.status(200).json({
      success: true,
      results: reader,
    });
  } catch (error) {
    console.error("Error in toArrowReader:", error);
    //TODO: handle errors

    res.status(500).json({
      success: false,
      results: null,
      error: error.message,
    });
  }
});

app.get("/esql/toarrowtable", async (__, res) => {
  const q = `FROM vet-visits 
  | KEEP owner_name, pet_name, species, breed, vaccination_history, visit_details 
  | LIMIT 5`;

  try {
    const table = await esClient.helpers.esql({ query: q }).toArrowTable();

    console.log("Table:", table);

    return res.status(200).json({
      success: true,
      results: table,
    });
  } catch (error) {
    console.error("Error in toArrowReader:", error);
    //TODO: handle errors

    res.status(500).json({
      success: false,
      results: null,
      error: error.message,
    });
  }
});

app.get("/esql/analysis", async (req, res) => {
  const { species, vaccine } = req.query;

  const q = `FROM vet-visits
    | WHERE  species == "${species}" AND match(vaccination_history, "${vaccine}")
    | KEEP owner_name, pet_name, species, breed, vaccination_history, visit_details
    | LIMIT 5`;

  console.log("Query:", q);

  try {
    const table = await esClient.helpers.esql({ query: q }).toRecords();

    console.log(table);

    return res.status(200).json({
      success: true,
      results: table,
    });
  } catch (error) {
    console.error("Error in toArrowReader:", error);
    //TODO: handle errors

    res.status(500).json({
      success: false,
      results: null,
      error: error.message,
    });
  }
});
