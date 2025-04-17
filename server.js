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
  serverMode: "serverless",
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
      size: 5,
      body: {
        query: {
          multi_match: {
            query: q,
            fields: ["owner_name", "pet_name", "visit_details"],
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
      let errorMessage =
        "Response error!, query malformed or server down, contact the administrator!";

      if (error.type === "parsing_exception") {
        errorMessage = "Query malformed, make sure mappings are set correctly";
      }

      res.status(error.meta.statusCode).json({
        erroStatus: error.meta.statusCode,
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

app.get("/search/semantic", async (req, res) => {
  const { q } = req.query;

  const INDEX_NAME = "vet-visits";

  try {
    const result = await esClient.search({
      index: INDEX_NAME,
      size: 5,
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

      res.status(error.meta.statusCode).json({
        erroStatus: error.meta.statusCode,
        success: false,
        results: null,
        error:
          "The request took more than 10s after 3 retries. Try again later.",
      });
    } else if (error instanceof errors.ResponseError) {
      res.status(500).json({
        success: false,
        results: null,
        error: error.body.error.reason,
      });
    }
  }
});

app.get("/search/hybrid", async (req, res) => {
  const { q } = req.query;

  // q = "nail trimming";

  const INDEX_NAME = "vet-visits";

  try {
    const result = await esClient.search({
      index: INDEX_NAME,
      size: 5,
      body: {
        retriever: {
          rrf: {
            retrievers: [
              {
                standard: {
                  query: {
                    bool: {
                      must: {
                        multi_match: {
                          query: q,
                          fields: ["owner_name", "pet_name", "visit_details"],
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
