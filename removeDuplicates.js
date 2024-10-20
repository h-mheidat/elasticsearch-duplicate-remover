const client = require('./elasticsearchClient');

/**
 * Finds duplicate databaseIds in the specified Elasticsearch index.
 *
 * @param {string} index - The name of the Elasticsearch index.
 * @return {Promise<string[]>} - A promise that resolves to an array of duplicate databaseIds.
 */
async function findDuplicates(index) {
  const response = await client.search({
    index,
    body: {
      size: 0,
      aggs: {
        duplicate_databaseIds: {
          terms: {
            field: 'databaseId',
            size: 1000000,
            min_doc_count: 2
          },
        },
      },
    },
  });

  return response.body.aggregations.duplicate_databaseIds.buckets.map(
    (bucket) => bucket.key
  );
}

/**
 * Retrieves documents associated with a specific databaseId in the given index.
 *
 * @param {string} index - The name of the Elasticsearch index.
 * @param {string} dbId - The databaseId to search for.
 * @return {Promise<object[]>} - A promise that resolves to an array of documents.
 */
async function getDocumentsToDelete(index, dbId) {
  const response = await client.search({
    index,
    body: {
      query: {
        term: {
          databaseId: dbId,
        },
      },
    },
  });

  return response.body.hits.hits;
}

/**
 * Deletes an array of documents from Elasticsearch.
 *
 * @param {object[]} docsToDelete - An array of documents to delete.
 * @return {Promise<void>} - A promise that resolves when the documents have been deleted.
 */
async function deleteDocuments(docsToDelete) {
  const body = docsToDelete.flatMap((doc) => [
    { delete: { _index: doc._index, _id: doc._id } },
  ]);

  if (body.length) {
    const { body: bulkResponse } = await client.bulk({ body });

    if (bulkResponse.errors) {
      console.error('Error deleting documents:', bulkResponse.errors);
    } else {
      console.log(`Deleted ${docsToDelete.length} documents.`);
    }
  }
}

/**
 * Removes duplicates from the specified Elasticsearch indices based on databaseId.
 *
 * @param {string[]} indices - An array of Elasticsearch index names.
 * @return {Promise<void>} - A promise that resolves when the operation is complete.
 */
async function removeDuplicatesFromIndices(indices) {
  for (const index of indices) {
    const duplicateIds = await findDuplicates(index);

    for (const dbId of duplicateIds) {
      const docs = await getDocumentsToDelete(index, dbId);

      if (docs.length > 1) {
        const keepDoc = docs[0]; // Keep the first document
        const docsToDelete = docs.slice(1); // All but the first
        await deleteDocuments(docsToDelete);
        console.log(
          `Deleted ${docsToDelete.length} duplicates for databaseId ${dbId} in index ${index}, kept document ID ${keepDoc._id}.`
        );
      }
    }
  }
}

// Usage example
const indicesToClean = ['elastic_search_en', 'elastic_search_er'];
removeDuplicatesFromIndices(indicesToClean).catch((err) => {
  console.error('Error:', err);
});
