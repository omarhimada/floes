using Amazon.Extensions.NETCore.Setup;
using Elasticsearch.Net;
using Elasticsearch.Net.Aws;
using Microsoft.Extensions.Logging;
using Nest;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FloES
{
    /// <summary>
    /// Wrapper for a Nest ElasticClient with simplified operations and built-in logging support
    /// </summary>
    public class Floe
    {
        /// <summary>
        /// The single instance of ElasticClient that lives as long as this Floe object does
        /// </summary>
        private readonly ElasticClient _client;

        #region Configurable private fields
        /// <summary>
        /// Optional logger
        /// </summary>
        private readonly ILogger<Floe> _logger;

        /// <summary>
        /// Collection of documents to write to an Elasticsearch index
        /// </summary>
        private readonly List<object> _documents = new List<object>();

        /// <summary>
        /// Default index to read/write the documents 
        /// </summary>
        private readonly string _defaultIndex;

        /// <summary>
        /// Whether or not to use a rolling date pattern for the indices
        /// </summary>
        private readonly bool _rollingDate;

        /// <summary>
        /// Default number of documents to write to Elasticsearch in each BulkRequest
        /// (5 is a safe number)
        /// </summary>
        // ReSharper disable once InconsistentNaming
        private const int _defaultNumberOfBulkDocumentsToWriteAtOnce = 5;

        /// <summary>
        /// Number of documents to write to Elasticsearch in each BulkRequest
        /// </summary>
        private readonly int _numberOfBulkDocumentsToWriteAtOnce;
        #endregion

        /// <summary>
        /// Build the 'Index' string for the BulkRequests
        /// </summary>
        private string IndexToWriteTo(string index = null)
        {
            string prefix = string.IsNullOrEmpty(index) ? _defaultIndex : index;
            string suffix = _rollingDate ? $"-{DateTime.UtcNow:yyyy.MM.dd}" : string.Empty;
            return $"{prefix}{suffix}";
        }

        /// <summary>
        /// Build the 'Index' string for the scrolls and searches
        /// </summary>
        private string IndexToSearch(string index = null) => string.IsNullOrEmpty(index) ? $"{_defaultIndex}*" : $"{index}*";

        #region Constructors
        /// <summary>
        /// Use this constructor if the ElasticClient has already been instantiated
        /// </summary>
        /// <param name="client"></param>
        /// <param name="defaultIndex">(Optional) default index to use for writing documents</param>
        /// <param name="numberOfBulkDocumentsToWriteAtOnce">
        ///     (Optional) number of documents to write to Elasticsearch
        ///     - set to 0 to write every record immediately, default is 5
        /// </param>
        /// <param name="rollingDate">(Optional) whether or not to use a rolling date pattern for the indices, default is false</param>
        /// <param name="logger">(Optional) ILogger to use</param>
        public Floe(
            ElasticClient client,
            string defaultIndex = null,
            int numberOfBulkDocumentsToWriteAtOnce = _defaultNumberOfBulkDocumentsToWriteAtOnce,
            bool rollingDate = false,
            ILogger<Floe> logger = null)
        {
            if (!string.IsNullOrEmpty(defaultIndex))
            {
                _defaultIndex = defaultIndex;
            }

            _rollingDate = rollingDate;

            if (numberOfBulkDocumentsToWriteAtOnce > 0)
            {
                _numberOfBulkDocumentsToWriteAtOnce = numberOfBulkDocumentsToWriteAtOnce;
            }

            _client = client;

            _logger = logger;
        }

        /// <summary>
        /// This constructor instantiates a new ElasticClient using the AWSOptions and AWS cluster URI
        /// </summary>
        /// <param name="awsOptions">AWSOptions containing the credentials and region endpoint</param>
        /// <param name="esClusterUri">URI of the Elasticsearch cluster in AWS</param>
        /// <param name="defaultIndex">(Optional) default index to use for writing documents</param>
        /// <param name="numberOfBulkDocumentsToWriteAtOnce">
        ///     (Optional) number of documents to write to Elasticsearch
        ///     - set to 0 to write every record immediately, default is 5
        /// </param>
        /// <param name="rollingDate">(Optional) whether or not to use a rolling date pattern for the indices, default is false</param>
        /// <param name="logger">(Optional) ILogger to use</param>
        public Floe(
            AWSOptions awsOptions,
            Uri esClusterUri,
            string defaultIndex = null,
            int numberOfBulkDocumentsToWriteAtOnce = _defaultNumberOfBulkDocumentsToWriteAtOnce,
            bool rollingDate = false,
            ILogger<Floe> logger = null)
        {
            if (awsOptions == null)
            {
                throw new ArgumentNullException(nameof(awsOptions));
            }

            if (!string.IsNullOrEmpty(defaultIndex))
            {
                _defaultIndex = defaultIndex;
            }

            _rollingDate = rollingDate;

            if (numberOfBulkDocumentsToWriteAtOnce > 0)
            {
                _numberOfBulkDocumentsToWriteAtOnce = numberOfBulkDocumentsToWriteAtOnce;
            }

            AwsHttpConnection httpConnection = new AwsHttpConnection(awsOptions);

            SingleNodeConnectionPool connectionPool = new SingleNodeConnectionPool(esClusterUri);
            ConnectionSettings connectionSettings = new ConnectionSettings(connectionPool, httpConnection);

            _client = new ElasticClient(connectionSettings);

            _logger = logger;
        }
        #endregion

        /// <summary>
        /// List all documents in an index asynchronously using the scroll API
        /// </summary>
        /// <typeparam name="T">Index POCO</typeparam>
        /// <param name="listToday">(Optional) whether or not to list using the rolling date of the index - default is false</param>
        /// <param name="scrollTime">(Optional) TTL of the scroll until another List is called - default is 60s</param>
        /// <param name="index">(Optional) index to scroll - if none provided the default index will be used</param>
        public async Task<IEnumerable<T>> List<T>(
          bool listToday = false,
          string scrollTime = "60s",
          string index = null) where T : class
        {
            string indexToScroll = IndexToSearch(index);
            ISearchResponse<T> searchResponse;
            if (!listToday)
            {
                searchResponse =
                  await _client.SearchAsync<T>(sd => sd
                    .Index(indexToScroll)
                    .From(0)
                    .Take(1000)
                    .MatchAll()
                    .Scroll(scrollTime));
            }
            else
            {
                // Scroll for the last day only (UTC)
                searchResponse =
                  await _client.SearchAsync<T>(sd => sd
                    .Index(indexToScroll)
                    .From(0)
                    .Take(1000)
                    .Query(query =>
                      query.DateRange(s => s
                        .Field("timeStamp")
                        .GreaterThanOrEquals(
                            DateTime.UtcNow.Subtract(TimeSpan.FromDays(1)))))
                    .Scroll(scrollTime));
            }

            List<T> results = new List<T>();

            bool continueScrolling = true;
            while (continueScrolling)
            {
                if (searchResponse.Documents != null && !searchResponse.IsValid || string.IsNullOrEmpty(searchResponse.ScrollId))
                {
                    _logger?.LogError($"~ ~ ~ Floe received an error while listing (scrolling) {searchResponse.ServerError.Error.Reason}");
                    break;
                }

                if (searchResponse.Documents != null && !searchResponse.Documents.Any())
                {
                    continueScrolling = false;
                }
                else
                {
                    results.AddRange(searchResponse.Documents);
                    searchResponse = await _client.ScrollAsync<T>(scrollTime, searchResponse.ScrollId);
                }
            }

            await _client.ClearScrollAsync(new ClearScrollRequest(searchResponse.ScrollId));

            return results;
        }

        /// <summary>
        /// Search for documents
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldToSearch">The name of the field in the document to search (e.g.: "customerId" or "animal.name")</param>
        /// <param name="valueToSearch">The value to search for</param>
        /// <param name="searchToday">(Optional) whether or not to limit the search to the rolling date - default is false</param>
        /// <param name="index">(Optional) index to search - if none provided the default index will be used</param>
        public async Task<IEnumerable<T>> Search<T>(
          string fieldToSearch,
          object valueToSearch,
          bool searchToday = false,
          string index = null) where T : class
        {
            string indexToSearch = IndexToSearch(index);

            ISearchResponse<T> searchResponse;
            if (!searchToday)
            {
                searchResponse =
                  await _client.SearchAsync<T>(s => s
                    .Size(10000)
                    .Index(indexToSearch)
                    .Query(q =>
                      q.Match(c => c
                        .Field(fieldToSearch)
                        .Query(valueToSearch.ToString()))));
            }
            else
            {
                searchResponse =
                  await _client.SearchAsync<T>(sd => sd
                    .Index(indexToSearch)
                    .Query(query =>
                      query.DateRange(s => s
                        .Field("timeStamp")
                        .GreaterThanOrEquals(
                          DateTime.UtcNow.Subtract(TimeSpan.FromDays(1))))));
            }

            if (searchResponse?.Documents != null && !searchResponse.IsValid)
            {
                return searchResponse.Documents;
            }

            _logger?.LogError($"~ ~ ~ Floe received an error while searching for [{fieldToSearch},{valueToSearch}]: {searchResponse?.ServerError.Error.Reason}");

            return null;
        }

        /// <summary>
        /// Write the document to an Elasticsearch index. Uses BulkAsync and 'numberOfBulkDocumentsToWriteAtOnce'
        /// </summary>
        /// <typeparam name="T">Index POCO</typeparam>
        /// <param name="document">Document to write to the Elasticsearch index</param>
        /// <param name="index">(Optional) index to write to - if none provided the default index will be used</param>
        public async Task Write<T>(
          T document,
          string index = null)
        {
            _documents.Add(document);

            string indexToWriteTo = IndexToWriteTo(index);

            try
            {
                // Ensure we are only making requests when we have enough documents
                if (_documents.Count >= _numberOfBulkDocumentsToWriteAtOnce)
                {
                    BulkDescriptor bulkDescriptor = new BulkDescriptor();
                    bulkDescriptor
                      .IndexMany(_documents)
                      .Index(indexToWriteTo);

                    BulkResponse bulkResponse = await _client.BulkAsync(bulkDescriptor);

                    if (bulkResponse.Errors)
                    {
                        string errorLogPrefix = $"~ ~ ~ Floe received an error while trying to write to index {indexToWriteTo}";

                        string errorMessage =
                          $"{errorLogPrefix}{Environment.NewLine}{JsonConvert.SerializeObject(bulkResponse.Errors)}";

                        _logger?.LogError(errorMessage);

                        throw new Exception(errorMessage);
                    }
                    else
                    {
                        _documents.Clear();
                    }
                }
            }
            catch (Exception exception)
            {
                string exceptionLogPrefix = $"~~~ Floe threw an exception while trying to write to index {indexToWriteTo}";

                string errorMessage =
                  $"{exceptionLogPrefix}{Environment.NewLine}{JsonConvert.SerializeObject(exception)}";

                _logger?.LogError(errorMessage);

                // ReSharper disable once PossibleIntendedRethrow
                throw exception;
            }
        }

        /// <summary>
        /// Find a document by its ID
        /// </summary>
        /// <typeparam name="T">Index POCO</typeparam>
        /// <param name="id">ID of the document</param>
        /// <returns>Null if no document was found</returns>
        public async Task<T> Find<T>(string id) where T : class
        {
            GetResponse<T> response = await _client.GetAsync<T>(id);

            if (response.Found)
            {
                _logger?.LogInformation($"~ ~ ~ Floe found document of type {typeof(T).Name} with ID {id}");

                return response.Source;
            }

            _logger?.LogInformation($"~ ~ ~ Floe could not find document of type {typeof(T).Name} with ID {id}");

            return null;
        }

        /// <summary>
        /// Delete all indices
        /// </summary>
        /// <returns>True if all indices were successfully deleted</returns>
        public async Task<bool> DeleteAllIndices()
        {
            _logger?.LogInformation($"~ ~ ~ Floe is deleting all indices");

            List<bool> indexDeletions = new List<bool>();
            foreach (KeyValuePair<IndexName, IndexState> index
            in (await _client.Indices.GetAsync(new GetIndexRequest(Indices.All))).Indices)
            {
                indexDeletions.Add(await DeleteIndex(index.Key.Name));
            }

            return indexDeletions.All(indexDeletion => true);
        }

        /// <summary>
        /// Delete an index
        /// </summary>
        /// <param name="index">Index name</param>
        public async Task<bool> DeleteIndex(string index)
        {
            if (!string.IsNullOrEmpty(index))
            {
                _logger?.LogInformation($"~ ~ ~ Floe is deleting index {index}");

                return (await _client.Indices.DeleteAsync(index)).IsValid;
            }

            return false;
        }
    }
}
