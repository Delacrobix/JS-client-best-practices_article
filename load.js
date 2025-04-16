const { Client } = require("@elastic/elasticsearch");
require("dotenv").config();

const ELASTICSEARCH_ENDPOINT = process.env.ELASTICSEARCH_ENDPOINT;
const ELASTICSEARCH_API_KEY = process.env.ELASTICSEARCH_API_KEY;

let esClient = new Client({
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
          copy_to: "semantic_field",
        },
        pet_name: {
          type: "text",
          copy_to: "semantic_field",
        },
        species: {
          type: "keyword",
          copy_to: "semantic_field",
        },
        breed: {
          type: "keyword",
          copy_to: "semantic_field",
        },
        vaccination_history: {
          type: "keyword",
          copy_to: "semantic_field",
        },
        visit_details: {
          type: "text",
          copy_to: "semantic_field",
        },
        semantic_field: {
          type: "semantic_text",
        },
      },
    });

    const documents = [
      {
        owner_name: "Alice Johnson",
        pet_name: "Buddy",
        species: "Dog",
        breed: "Golden Retriever",
        vaccination_history: ["Rabies", "Parvovirus", "Distemper"],
        visit_details: "Annual check-up and nail trimming. Healthy and active.",
      },
      {
        owner_name: "Marco Rivera",
        pet_name: "Milo",
        species: "Cat",
        breed: "Siamese",
        vaccination_history: ["Rabies", "Feline Leukemia"],
        visit_details: "Slight eye irritation, prescribed eye drops.",
      },
      {
        owner_name: "Sandra Lee",
        pet_name: "Pickles",
        species: "Guinea Pig",
        breed: "Mixed",
        vaccination_history: [],
        visit_details: "Loss of appetite, recommended dietary changes.",
      },
      {
        owner_name: "Jake Thompson",
        pet_name: "Luna",
        species: "Dog",
        breed: "Labrador Mix",
        vaccination_history: ["Rabies", "Bordetella"],
        visit_details: "Mild ear infection, cleaning and antibiotics given.",
      },
      {
        owner_name: "Emily Chen",
        pet_name: "Ziggy",
        species: "Cat",
        breed: "Mixed",
        vaccination_history: ["Rabies", "Feline Calicivirus"],
        visit_details: "Vaccination update and routine physical.",
      },
      {
        owner_name: "Tomás Herrera",
        pet_name: "Rex",
        species: "Dog",
        breed: "German Shepherd",
        vaccination_history: ["Rabies", "Parvovirus", "Leptospirosis"],
        visit_details: "Follow-up for previous leg strain, improving well.",
      },
      {
        owner_name: "Nina Park",
        pet_name: "Coco",
        species: "Ferret",
        breed: "Mixed",
        vaccination_history: ["Rabies"],
        visit_details: "Slight weight loss; advised new diet.",
      },
      {
        owner_name: "Leo Martínez",
        pet_name: "Simba",
        species: "Cat",
        breed: "Maine Coon",
        vaccination_history: ["Rabies", "Feline Panleukopenia"],
        visit_details: "Dental cleaning. Minor tartar buildup removed.",
      },
      {
        owner_name: "Rachel Green",
        pet_name: "Rocky",
        species: "Dog",
        breed: "Bulldog Mix",
        vaccination_history: ["Rabies", "Parvovirus"],
        visit_details: "Skin rash, antihistamines prescribed.",
      },
      {
        owner_name: "Daniel Kim",
        pet_name: "Mochi",
        species: "Rabbit",
        breed: "Mixed",
        vaccination_history: [],
        visit_details: "Nail trimming and general health check. No issues.",
      },
    ];

    await indexData(documents, INDEX_NAME);
  } catch (error) {
    console.error("Error in main function:", error);

    throw error;
  }
};

const createMappings = async (indexName, mapping) => {
  try {
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

const buildBulkData = (documents, indexName) => {
  const operations = [];

  for (const doc of documents) {
    operations.push({
      index: { _index: indexName },
    });
    operations.push(doc);
  }

  return operations;
};

const indexData = async (documents, indexName) => {
  try {
    const operations = buildBulkData(documents, indexName);
    const body = await esClient.bulk({ refresh: true, operations });

    console.log("Bulk indexing successful elements:", body.items.length);
  } catch (error) {
    console.error("Error indexing data:", error);
    throw error;
  }
};

// Execute the main function
main();
