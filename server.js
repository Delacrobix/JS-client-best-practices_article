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

app.get("/search/lexic", async (req, res) => {
  const { q } = req.query;

  const INDEX_NAME = "vet-visits";

  try {
    const result = await esClient.search({
      index: INDEX_NAME,
      body: {
        query: {
          terms: {
            visit_details: q,
            // visit_details: "nail trimming",
          },
        },
      },
    });

    res.status(200).json({
      success: true,
      results: result.hits.hits,
    });
  } catch (error) {
    if (error instanceof errors.ResponseError) {
      console.error("Response error:", error.body);

      res.status(500).json({
        success: false,
        results: null,
        error: error.body.error.reason,
      });
    }
  }
});

app.get("/search/semantic", async (req, res) => {
  const { q } = req.query;

  const INDEX_NAME = "vet-visits";

  try {
    const result = await esClient.search({
      index: INDEX_NAME,
      body: {
        query: {
          semantic: {
            field: "semantic_field",
            query: q,
            // query: "Which pets had nail trimming?",
          },
        },
      },
    });

    res.status(200).json({
      success: true,
      results: result.hits.hits,
    });
  } catch (error) {
    if (error instanceof errors.TimeoutError) {
      console.error("Timeout error:", error.body);

      res.status(500).json({
        success: false,
        results: null,
        error: error.body.error.reason,
      });
    }

    res.status(500).json({
      success: false,
      results: null,
      error: error.message,
    });
  }
});

app.get("/search/hybrid", async (req, res) => {
  const { q } = req.query;

  // q = "nail trimming";

  const INDEX_NAME = "vet-visits";

  try {
    const result = await esClient.search({
      index: INDEX_NAME,
      body: {
        retriever: {
          rrf: {
            retrievers: [
              {
                standard: {
                  query: {
                    bool: {
                      must: {
                        match: {
                          visit_details: q,
                        },
                      },
                    },
                  },
                },
              },
              {
                standard: {
                  query: {
                    bool: {
                      must: {
                        semantic: {
                          field: "semantic_field",
                          query: q,
                        },
                      },
                    },
                  },
                },
              },
            ],
          },
        },
        size: 10,
      },
    });

    res.status(200).json({
      success: true,
      results: result.hits.hits,
    });
  } catch (error) {
    console.error("Error performing search:", error);

    res.status(500).json({
      success: false,
      results: null,
      error: error.message,
    });
  }
});
