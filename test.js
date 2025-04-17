const { Client, errors } = require("@elastic/elasticsearch");
const Mock = require("@elastic/elasticsearch-mock");
const test = require("ava");

const mock = new Mock();
const esClient = new Client({
  node: "http://localhost:9200",
  Connection: mock.getConnection(),
});

function createSemanticSearchMock(query, indexName) {
  mock.add(
    {
      method: "POST",
      path: `/${indexName}/_search`,
      body: {
        query: {
          semantic: {
            field: "semantic_field",
            query: query,
          },
        },
      },
    },
    () => {
      return {
        hits: {
          total: { value: 2, relation: "eq" },
          hits: [
            {
              _id: "1",
              _score: 0.9,
              _source: {
                owner_name: "Alice Johnson",
                pet_name: "Buddy",
                species: "Dog",
                breed: "Golden Retriever",
                vaccination_history: ["Rabies", "Parvovirus", "Distemper"],
                visit_details:
                  "Annual check-up and nail trimming. Healthy and active.",
              },
            },
            {
              _id: "2",
              _score: 0.7,
              _source: {
                owner_name: "Daniel Kim",
                pet_name: "Mochi",
                species: "Rabbit",
                breed: "Mixed",
                vaccination_history: [],
                visit_details:
                  "Nail trimming and general health check. No issues.",
              },
            },
          ],
        },
      };
    }
  );
}

test("performSemanticSearch must return formatted results correctly", async (t) => {
  const indexName = "vet-visits";
  const query = "Which pets had nail trimming?";

  createSemanticSearchMock(query, indexName);

  async function performSemanticSearch(esClient, q, indexName = "vet-visits") {
    try {
      const result = await esClient.search({
        index: indexName,
        body: {
          query: {
            semantic: {
              field: "semantic_field",
              query: q,
            },
          },
        },
      });

      return {
        success: true,
        results: result.hits.hits,
      };
    } catch (error) {
      if (error instanceof errors.TimeoutError) {
        return {
          success: false,
          results: null,
          error: error.body.error.reason,
        };
      }

      return {
        success: false,
        results: null,
        error: error.message,
      };
    }
  }

  const result = await performSemanticSearch(esClient, query, indexName);

  t.true(result.success, "The search must be successful");
  t.true(Array.isArray(result.results), "The results must be an array");

  if (result.results.length > 0) {
    t.true(
      "_source" in result.results[0],
      "Each result must have a _source property"
    );
    t.true(
      "pet_name" in result.results[0]._source,
      "Results must include the pet_name field"
    );
    t.true(
      "visit_details" in result.results[0]._source,
      "Results must include the visit_details field"
    );
  }
});

// test("createMappings must configure the index", async (t) => {
//   const indexName = "vet-visits";

//   let capturedBody = null;

//   mock.add(
//     {
//       method: "PUT",
//       path: `/${indexName}/_mapping`,
//     },
//     (params) => {
//       capturedBody = params.body;
//       return { acknowledged: true };
//     }
//   );

//   async function createMappings(client, indexName, mappings) {
//     return await client.indices.putMapping({
//       index: indexName,
//       body: mappings,
//     });
//   }

//   const mappingsConfig = {
//     properties: {
//       owner_name: { type: "text", copy_to: "semantic_field" },
//       pet_name: { type: "text", copy_to: "semantic_field" },
//       species: { type: "keyword", copy_to: "semantic_field" },
//       breed: { type: "keyword", copy_to: "semantic_field" },
//       vaccination_history: { type: "keyword", copy_to: "semantic_field" },
//       visit_details: { type: "text", copy_to: "semantic_field" },
//       semantic_field: { type: "semantic_text" },
//     },
//   };

//   const response = await createMappings(esClient, indexName, mappingsConfig);

//   t.deepEqual(capturedBody, mappingsConfig, "The mappings must be correct");
//   t.truthy(response.acknowledged, "The response must be acknowledged");
// });
