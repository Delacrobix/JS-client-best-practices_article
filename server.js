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
  try {
    const q = `FROM kibana_sample_data_logs 
  | KEEP message, response, tags, @timestamp, ip, agent 
  | LIMIT 2 `;

    const results = await esClient.helpers.esql({ query: q }).toRecords();

    res.status(200).json({
      success: true,
      results: results,
    });
  } catch (error) {
    if (error instanceof errors.ResponseError) {
      let errorMessage =
        "Response error! query malformed or server down. Contact the administrator";

      if (error.body.error.type === "parsing_exception") {
        errorMessage = "Query malformed! Please check your query syntax.";
      }

      res.status(error.meta.statusCode).json({
        success: false,
        results: null,
        error: errorMessage,
      });
    }

    res.status(500).json({
      success: false,
      results: null,
      error: error.message,
    });
  }
});

app.get("/esql/toarrowreader", async (__, res) => {
  const q = `FROM kibana_sample_data_logs 
  | KEEP message, response, tags, @timestamp, ip, agent 
  | LIMIT 2 `;

  try {
    const reader = await esClient.helpers.esql({ query: q }).toArrowReader();

    const results = [];

    for await (const recordBatch of reader) {
      for (const record of recordBatch) {
        const recordData = record.toJSON();

        results.push(recordData);
      }
    }

    res.status(200).json({
      success: true,
      results: results,
    });
  } catch (error) {
    if (error instanceof errors.ResponseError) {
      let errorMessage =
        "Response error! query malformed or server down. Contact the administrator";

      if (error.body.error.type === "parsing_exception") {
        errorMessage = "Query malformed! Please check your query syntax.";
      }

      res.status(error.meta.statusCode).json({
        success: false,
        results: null,
        error: errorMessage,
      });
    }

    res.status(500).json({
      success: false,
      results: null,
      error: error.message,
    });
  }
});

app.get("/esql/toarrowtable", async (__, res) => {
  const q = `FROM kibana_sample_data_logs 
  | KEEP message, response, tags, @timestamp, ip, agent 
  | LIMIT 2 `;

  try {
    const table = await esClient.helpers.esql({ query: q }).toArrowTable();

    const arrayTable = table.toArray();

    res.status(200).json({
      success: true,
      results: arrayTable,
    });
  } catch (error) {
    if (error instanceof errors.ResponseError) {
      let errorMessage =
        "Response error! query malformed or server down. Contact the administrator";

      if (error.body.error.type === "parsing_exception") {
        errorMessage = "Query malformed! Please check your query syntax.";
      }

      res.status(error.meta.statusCode).json({
        success: false,
        results: null,
        error: errorMessage,
      });
    }

    res.status(500).json({
      success: false,
      results: null,
      error: error.message,
    });
  }
});

app.get("/esql/analysis", async (req, res) => {
  const { localization, startDate, endDate } = req.query;

  const q = `FROM kibana_sample_data_logs
    | WHERE geo.src == "${localization}" AND @timestamp >= "${startDate}" AND @timestamp <= "${endDate}"
    | EVAL log_date = DATE_FORMAT("yyyy-MM-dd", @timestamp)
    | KEEP geo.dest, geo.src, bytes, log_date
    | SORT log_date DESC
    | LIMIT 5
  `;

  try {
    const results = await esClient.helpers.esql({ query: q }).toRecords();

    res.status(200).json({
      success: true,
      results: results,
    });
  } catch (error) {
    console.log(error);

    if (error instanceof errors.ResponseError) {
      let errorMessage =
        "Response error! query malformed or server down. Contact the administrator";

      if (error.body.error.type === "parsing_exception") {
        errorMessage = "Query malformed! Please check your query syntax.";
      }

      res.status(500).json({
        success: false,
        results: null,
        error: errorMessage,
      });
    }

    res.status(500).json({
      success: false,
      results: null,
      error: error.message,
    });
  }
});
