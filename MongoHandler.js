const { MongoClient } = require('mongodb');

class MongoHandler {
  constructor(url, databaseName) {
    this.url = url;
    this.dbInstance = null;
    this.client = null;
    this.databaseName = databaseName;
  }

  async connect() {
    try {
      // Connect to MongoDB with the appropriate options (useUnifiedTopology and useNewUrlParser)
      this.client = await MongoClient.connect(this.url, { 
        useUnifiedTopology: true, 
        useNewUrlParser: true 
      });
      this.dbInstance = this.client.db(this.databaseName);
    } catch (err) {
      throw new Error(`Failed to connect to MongoDB: ${err.message}`);
    }
  }

  // Find all documents in a collection
  async findAll(collectionName) {
    try {
      const documents = await this.dbInstance.collection(collectionName).find({}).toArray();
      return documents || [];
    } catch (err) {
      throw new Error(`Error finding documents in ${collectionName}: ${err.message}`);
    }
  }

  // Save all documents in a collection, replacing existing ones
  async saveAll(collectionName, objects) {
    try {
      // Drop the collection if it exists before saving
      await this.dropCollectionIfExists(collectionName);

      if (objects.length > 0) {
        const bulk = this.dbInstance.collection(collectionName).initializeUnorderedBulkOp();
        objects.forEach((obj) => bulk.insert(obj));
        await bulk.execute();
      }

    } catch (err) {
      throw new Error(`Error saving documents to ${collectionName}: ${err.message}`);
    }
  }

  // Drop a collection if it exists
  async dropCollectionIfExists(collectionName) {
    const collectionList = await this.dbInstance.listCollections({ name: collectionName }).toArray();
    if (collectionList.length !== 0) {
      await this.dbInstance.collection(collectionName).drop();
    }
  }

  // Find a single document by its path
  async findOneByPath(collectionName, path) {
    try {
      const document = await this.dbInstance.collection(collectionName).findOne({ path });
      if (document) {
        return document.body ? JSON.parse(document.body) : {};
      }
      return {};
    } catch (err) {
      throw new Error(`Error finding document with path ${path} in ${collectionName}: ${err.message}`);
    }
  }

  // Save or update a document by its path
  async saveOrUpdateByPath(collectionName, path, meta, body) {
    try {
      let storageDocument = await this.dbInstance.collection(collectionName).findOne({ path });
      
      if (!storageDocument) {
        storageDocument = { path };
      }

      storageDocument.meta = JSON.stringify(meta);
      storageDocument.body = JSON.stringify(body);

      // MongoDB save equivalent for updating or inserting
      await this.dbInstance.collection(collectionName).replaceOne({ path }, storageDocument, { upsert: true });

    } catch (err) {
      throw new Error(`Error saving or updating document with path ${path} in ${collectionName}: ${err.message}`);
    }
  }
}

module.exports = MongoHandler;
