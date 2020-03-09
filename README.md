# FloES
FloES is a generic wrapper for common Elasticsearch operations, such as writing and reading documents, using Nest & Elasticsearch.Net.AWS. Includes ILogger support

**https://www.nuget.org/packages/FloES**

**Example usage:**
````C#
// Your AWSOptions
AWSOptions awsOptions = new AWSOptions
{
    Credentials = new BasicAWSCredentials(_config.AccessKey, _config.SecretAccessKey),
    Region = Amazon.RegionEndpoint.CACentral1
};

// Instantiate a new Floe
_ordersFloe = new Floe(
    awsOptions: awsOptions,
    esClusterUri: new Uri(_config.AwsElasticsearchEndpoint),
    defaultIndex: "idx-orders",
    numberOfBulkDocumentsToWriteAtOnce: 0,
    rollingDate: true);
    
// Use the Floe to write an 'Order' document to the default index with a rolling date (e.g.: "idx-orders-2020-03-06")
_ordersFloe.Write<Order>(order);

// Use the Floe to get a document
Order order = _ordersFloe.Find<Order>(id: "1");
````
**Example writing many documents asynchronously:**
````C#
// WriteMany uses Task.WaitAll and the Write method to write many documents asynchronously
await _ordersFloe.WriteMany<IEnumerable<Order>>(collectionOfOrders);
````
