# FloES
FloES is a generic wrapper for common Elasticsearch operations, such as writing and reading documents, using Nest & Elasticsearch.Net.AWS. Includes ILogger support

**https://www.nuget.org/packages/FloES**

**Example usage: write, find, list and search for 'Orders' (e.g.: eCommerce website)**
````C#
// Your AWSOptions
AWSOptions awsOptions = new AWSOptions
{
    Credentials = new BasicAWSCredentials(_config.AccessKey, _config.SecretAccessKey),
    Region = Amazon.RegionEndpoint.CACentral1
};

// Instantiate a new Floe for our 'Order' documents
_ordersFloe = new Floe<ExampleOrdersService>(
    awsOptions: awsOptions,
    esClusterUri: new Uri(_config.AwsElasticsearchEndpoint),
    defaultIndex: "idx-orders",
    logger: _logger, // optionally pass in your ILogger to get automatic logs
    numberOfBulkDocumentsToWriteAtOnce: 3, // pick a higher number if you're writing lots of documents very rapidly
    rollingDate: true);
    
// Write an order document to the default index with a rolling date (e.g.: "idx-orders-2020-03-06")
// You can write many asynchronously by calling this in a loop (safe due to BulkAsync usage with a smart numberOfBulkDocumentsToWriteAtOnce choice)
await _ordersFloe.Write<Order>(order);

// Get an order
Order order = await _ordersFloe.Find<Order>(id: "1");

// List all orders
IEnumerable<Order> orders = await _ordersFloe.List<Order>();

// List all orders for the last 24 hours
IEnumerable<Order> orders = await _ordersFloe.List<Order>(listLast24Hours: true);

// Search for orders of SKU 100
IEnumerable<Order> orders = await _ordersFloe.Search<Order>("sku", 100);

// Delete all indices and then dispose of the Floe capable of doing so
{
    await using Floe<ExampleAdminService> temporaryDeleteAllIndicesFloe = new Floe(
      awsOptions: _awsOptions,
      esClusterUri: new Uri(_config.AwsElasticsearchEndpoint));

    await temporaryDeleteIndexFloe.DeleteAllIndices();
}
````
**Help! I'm writing duplicates!**
Make sure the document object you're writing has a unique "Id" parameter. Because of the asynchronous nature of `.Write`, and Elasticsearch clustering, by allowing Elasticsearch to automatically generate an "Id" parameter you run the risk of creating duplicate documents with their own unique IDs. An example is below:
````C#
// Class definition
public partial class Log 
{
    [JsonProperty("id")]
    public string Id { get; set; }
    
    // ...
}

// Document
Log log = new Log
{
    Id = $"Log-{task}-{now.ToString(CultureInfo.InvariantCulture)}",
    TaskName = task,
    Description = description,
};

// No duplicates will be created since we are specifying the ID ourselves
await _logsFloe.Write<Log>(log);
````
