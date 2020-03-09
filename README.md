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
    
// Write an 'Order' document to the default index with a rolling date (e.g.: "idx-orders-2020-03-06")
await _ordersFloe.Write<Order>(order);

// Write many orders asynchronously
await _ordersFloe.WriteMany<Order>(collectionOfOrders);

// Get an order
Order order = await _ordersFloe.Find<Order>(id: "1");

// List all orders
IEnumerable<Order> orders = await _ordersFloe.List<Order>();

// List all orders for today
IEnumerable<Order> orders = await _ordersFloe.List<Order>(listToday: true);

// Search for orders of SKU 100
IEnumerable<Order> orders = await _ordersFloe.Search<Order>("SKU", 100);
