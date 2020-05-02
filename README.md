# FloES
FloES is a generic wrapper for common Elasticsearch operations, such as writing and reading documents, using Nest & Elasticsearch.Net.AWS. Includes ILogger support

**https://www.nuget.org/packages/FloES**

**Example usage: instantiation (e.g.: eCommerce website)**
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
    rollingDate: true); // documents will be written to indices with rolling dates (e.g.: idx-orders-2020-04-20)
````

**Example usage: write, find 'Orders'**
````C#    
// Write an order document to the default index with a rolling date (e.g.: idx-orders-2020-04-20)
// You can write many asynchronously by calling this in a loop (safe due to BulkAsync usage, with a smart numberOfBulkDocumentsToWriteAtOnce choice)
await _ordersFloe.Write<Order>(order);

// Choosing a good numberOfBulkDocumentsToWriteAtOnce:
// Writing 10,000 documents/hour -> numberOfBulkDocumentsToWriteAtOnce: ~50
// Writing 3 documents a day -> numberOfBulkDocumentsToWriteAtOnce: ~1
// tl;dr: use your head!

// Write any remaining unwritten documents from the buffer (e.g.: call this once after a very long loop to finish up)
await _ordersFloe.WriteUnwritten<Order>();

// Get an order
Order order = await _ordersFloe.Find<Order>(id: "1");

````

**Example usage: listing and searching**
````C#

// List all orders
IEnumerable<Order> orders = await _ordersFloe.List<Order>();

// List all orders for the last 24 hours
IEnumerable<Order> orders = await _ordersFloe.List<Order>(listLastXHours: 24);

// List all orders for the last 7.5 days
IEnumerable<Order> orders = await _ordersFloe.List<Order>(listLastXDays: 7.5);

// Search for orders of SKU 100
IEnumerable<Order> orders = await _ordersFloe.Search<Order>("sku", 100);

// Search for orders of SKU 100 for the last 4.5 hours
IEnumerable<Order> orders = await _ordersFloe.Search<Order>(
    fieldToSearch: "sku", 
    valueToSearch: 100,
    listLastXHours: 4.5);
````
    
**Example usage: scrolling manually (i.e.: use this if you want to do some operation during the scroll. Otherwise just use Search or List**
````C#
// Begin a scroll for all orders in Canada for the last year, getting 1000 orders at a time
ISearchResponse<Order> scrollCanada = await _ordersFloe.BeginScroll<Order>(
    scrollForXDocuments: 1000,
    listLastXDays: 365.25);
    
bool continueScrolling = true;
while (continueScrolling && searchResponse != null)
{
    if (scrollCanada.Documents != null && !scrollCanada.IsValid)
    {
        break;
    }

    if (scrollCanada.Documents != null && !scrollCanada.Documents.Any())
    {
        continueScrolling = false;
    }
    else
    {
        // Do something with your 1000 orders before continuing the scroll
        _yourResults.AddRange(scrollCanada.Documents);
        _yourProgressIndicator.IndicateScrollProgress(yourResults.Count);
        _yourLogger.LogInformation($"We got another 1000 orders from Elasticsearch!");
        
        // Continue the scroll for the next set of orders
        scrollCanada = await _ordersFloe.ContinueScroll<Order>(scrollCanada);
    }
}

// End the scroll
await _ordersFloe.EndScroll(scrollCanada);
````

**Example usage: delete test indices when debugging on development environment**
````C#
// DANGER: delete all indices and then dispose of the Floe capable of doing so
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

// Document we want to write (e.g.: some Log)
Log log = new Log
{
    Id = $"log-{task}-{DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss)}",
    TaskName = task,
    Description = description,
};

// No duplicates will be created since we are specifying the ID ourselves
await _logsFloe.Write<Log>(log);
````
