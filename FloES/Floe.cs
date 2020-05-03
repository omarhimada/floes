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
    /// Wrapper for a Nest ElasticClient with common Elasticsearch operations
    /// - can use with 'await using', and includes ILogger support
    /// </summary>
    public class Floe<T> : IAsyncDisposable, IDisposable
    {
        private bool _disposed;

        /// <summary>
        /// Optional logger
        /// </summary>
        private readonly ILogger<T> _logger;

        /// <summary>
        /// The single instance of ElasticClient that lives as long as this Floe object does
        /// </summary>
        private readonly ElasticClient _client;

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
        /// <param name="rollingDate">(Optional) whether or not to use a rolling date pattern when writing to indices, default is false</param>
        /// <param name="logger">(Optional) ILogger to use</param>
        public Floe(
            ElasticClient client,
            string defaultIndex = null,
            int numberOfBulkDocumentsToWriteAtOnce = _defaultNumberOfBulkDocumentsToWriteAtOnce,
            bool rollingDate = false,
            ILogger<T> logger = null)
        {
            if (!string.IsNullOrEmpty(defaultIndex))
            {
                _defaultIndex = defaultIndex;
            }

            _rollingDate = rollingDate;

            if (numberOfBulkDocumentsToWriteAtOnce > -1)
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
        /// <param name="rollingDate">(Optional) whether or not to use a rolling date pattern when writing to indices, default is false</param>
        /// <param name="logger">(Optional) ILogger to use</param>
        public Floe(
            AWSOptions awsOptions,
            Uri esClusterUri,
            string defaultIndex = null,
            int numberOfBulkDocumentsToWriteAtOnce = _defaultNumberOfBulkDocumentsToWriteAtOnce,
            bool rollingDate = false,
            ILogger<T> logger = null)
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

            if (numberOfBulkDocumentsToWriteAtOnce > -1)
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
        /// <param name="listLastXHours">(Optional) whether or not to list using the last X hours of the UTC date - default is null</param>
        /// <param name="listLastXDays">(Optional) whether or not to list using the last X days of the UTC date - default is null</param>
        /// <param name="scrollTime">(Optional) TTL of the scroll until another List is called - default is 60s</param>
        /// <param name="index">(Optional) index to scroll - if none provided the default index will be used</param>
        /// <param name="timeStampField">(Optional) the name of the time stamp field to consider - default is "timeStamp"</param>
        public async Task<IEnumerable<T>> List<T>(
          double? listLastXHours = null,
          double? listLastXDays = null,
          string scrollTime = "60s",
          string index = null,
          string timeStampField = "timeStamp") where T : class
        {
            string indexToScroll = IndexToSearch(index);
            ISearchResponse<T> searchResponse = null;
            List<T> results = new List<T>();

            if (listLastXHours == null && listLastXDays == null)
            {
                searchResponse =
                  await _client.SearchAsync<T>(sd => sd
                    .Index(indexToScroll)
                    .From(0)
                    .Take(1000)
                    .MatchAll()
                    .Scroll(scrollTime));
            }
            else if (listLastXHours != null)
            {
                // Prevent attempt to scroll over a TimeSpan of zero
                if (listLastXHours == 0)
                {
                    listLastXHours = 1;
                }

                // Scroll for the last X hours only (UTC)
                searchResponse =
                  await _client.SearchAsync<T>(sd => sd
                    .Index(indexToScroll)
                    .From(0)
                    .Take(1000)
                    .Query(query =>
                      query.DateRange(s => s
                        .Field(timeStampField)
                        .GreaterThanOrEquals(
                            DateTime.UtcNow.Subtract(TimeSpan.FromHours(listLastXHours.Value)))))
                    .Scroll(scrollTime));
            }
            else if (listLastXDays != null)
            {
                // Prevent attempt to scroll over a TimeSpan of zero
                if (listLastXDays == 0)
                {
                    listLastXDays = 1;
                }

                // Scroll for the last X days only (UTC)
                searchResponse =
                  await _client.SearchAsync<T>(sd => sd
                    .Index(indexToScroll)
                    .From(0)
                    .Take(1000)
                    .Query(query =>
                      query.DateRange(s => s
                        .Field(timeStampField)
                        .GreaterThanOrEquals(
                            DateTime.UtcNow.Subtract(TimeSpan.FromDays(listLastXDays.Value)))))
                    .Scroll(scrollTime));
            }

            if (searchResponse == null || string.IsNullOrEmpty(searchResponse.ScrollId))
            {
                _logger?.LogInformation($"~ ~ ~ Floe received a null search response or failed to scroll (index may not exist)");
                return results;
            }

            bool continueScrolling = true;
            while (continueScrolling && searchResponse != null)
            {
                if (searchResponse.Documents != null && !searchResponse.IsValid)
                {
                    _logger?.LogError($"~ ~ ~ Floe received an error while listing (scrolling) {searchResponse.ServerError?.Error?.Reason}");
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
        /// <param name="searchLastXHours">(Optional) whether or not to search using the last X hours of the UTC date - default is null</param>
        /// <param name="searchLastXDays">(Optional) whether or not to search using the last X days of the UTC date - default is null</param>
        /// <param name="scrollTime">(Optional) TTL of the scroll until another List is called - default is 60s</param>
        /// <param name="index">(Optional) index to search - if none provided the default index will be used</param>
        /// <param name="timeStampField">(Optional) the name of the time stamp field to consider - default is "timeStamp"</param>
        public async Task<IEnumerable<T>> Search<T>(
          string fieldToSearch,
          object valueToSearch,
          double? searchLastXHours = null,
          double? searchLastXDays = null,
          string scrollTime = "60s",
          string index = null,
          string timeStampField = "timeStamp") where T : class
        {
            string indexToScroll = IndexToSearch(index);
            ISearchResponse<T> searchResponse = null;
            List<T> results = new List<T>();

            if (searchLastXHours == null && searchLastXDays == null)
            {
                searchResponse =
                  await _client.SearchAsync<T>(sd => sd
                    .Index(indexToScroll)
                    .From(0)
                    .Take(1000)
                    .Query(q =>
                      q.Match(c => c
                        .Field(fieldToSearch)
                        .Query(valueToSearch.ToString())))
                    .Scroll(scrollTime));
            }
            else if (searchLastXHours != null)
            {
                // Prevent attempt to scroll over a TimeSpan of zero
                if (searchLastXHours == 0)
                {
                    searchLastXHours = 1;
                }

                // Scroll for the last X hours only (UTC)
                searchResponse =
                  await _client.SearchAsync<T>(sd => sd
                    .Index(indexToScroll)
                    .From(0)
                    .Take(1000)
                    .Query(query =>
                      query
                        .DateRange(s => s
                        .Field(timeStampField)
                        .GreaterThanOrEquals(DateTime.UtcNow.Subtract(TimeSpan.FromDays(searchLastXHours.Value)))))
                    .Query(query =>
                      query.Match(c => c
                        .Field(fieldToSearch)
                        .Query(valueToSearch.ToString())))
                    .Scroll(scrollTime));
            }
            else if (searchLastXDays != null)
            {
                // Prevent attempt to scroll over a TimeSpan of zero
                if (searchLastXDays == 0)
                {
                    searchLastXDays = 1;
                }

                // Scroll for the last X days only (UTC)
                searchResponse =
                  await _client.SearchAsync<T>(sd => sd
                    .Index(indexToScroll)
                    .From(0)
                    .Take(1000)
                    .Query(query =>
                      query.DateRange(s => s
                        .Field(timeStampField)
                        .GreaterThanOrEquals(
                            DateTime.UtcNow.Subtract(TimeSpan.FromDays(searchLastXDays.Value)))))
                    .Query(query =>
                      query.Match(c => c
                        .Field(fieldToSearch)
                        .Query(valueToSearch.ToString())))
                    .Scroll(scrollTime));
            }

            if (searchResponse == null || string.IsNullOrEmpty(searchResponse.ScrollId))
            {
                _logger?.LogInformation($"~ ~ ~ Floe received a null search response or failed to scroll (index may not exist)");
                return results;
            }

            bool continueScrolling = true;
            while (continueScrolling && searchResponse != null)
            {
                if (searchResponse.Documents != null && !searchResponse.IsValid)
                {
                    _logger?.LogError($"~ ~ ~ Floe received an error while searching (scrolling) {searchResponse.ServerError?.Error?.Reason}");
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
        /// Begin a scroll and return the ISearchResponse so you can scroll yourself. Use this if you want
        /// to do some logic between every response of X number of documents
        /// (NOTE:
        /// this is essentially the implementation for 'Search' except the scroll is exposed to you,
        /// you should call EndScroll when you are done scrolling.)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldToSearch">(Optional) the name of the field in the document to search (e.g.: "customerId" or "animal.name") - default is null</param>
        /// <param name="valueToSearch">(Optional) the value to search for - default is null</param>
        /// <param name="scrollForXDocuments">(Optional) how many documents you will be scrolling at a time - default is 1000</param>
        /// <param name="scrollLastXHours">(Optional) whether or not to scroll using the last X hours of the UTC date - default is null</param>
        /// <param name="scrollLastXDays">(Optional) whether or not to scroll using the last X days of the UTC date - default is null</param>
        /// <param name="scrollTime">(Optional) TTL of the scroll until another List is called - default is 60s</param>
        /// <param name="index">(Optional) index to search - if none provided the default index will be used</param>
        /// <param name="timeStampField">(Optional) the name of the time stamp field to consider - default is "timeStamp"</param>
        public async Task<ISearchResponse<T>> BeginScroll<T>(
          string fieldToSearch = null,
          object valueToSearch = null,
          int scrollForXDocuments = 1000,
          double? scrollLastXHours = null,
          double? scrollLastXDays = null,
          string scrollTime = "60s",
          string index = null,
          string timeStampField = "timeStamp") where T : class
        {
            string indexToScroll = IndexToSearch(index);
            ISearchResponse<T> searchResponse = null;

            if (scrollLastXHours == null && scrollLastXDays == null)
            {
                if (fieldToSearch != null && valueToSearch != null)
                {
                    searchResponse =
                      await _client.SearchAsync<T>(sd => sd
                        .Index(indexToScroll)
                        .From(0)
                        .Take(scrollForXDocuments)
                        .Query(q =>
                          q.Match(c => c
                            .Field(fieldToSearch)
                            .Query(valueToSearch.ToString())))
                        .Scroll(scrollTime));
                }
                else
                {
                    searchResponse =
                      await _client.SearchAsync<T>(sd => sd
                        .Index(indexToScroll)
                        .From(0)
                        .Take(scrollForXDocuments)
                        .MatchAll()
                        .Scroll(scrollTime));
                }
            }
            else if (scrollLastXHours != null)
            {
                // Prevent attempt to scroll over a TimeSpan of zero
                if (scrollLastXHours == 0)
                {
                    scrollLastXHours = 1;
                }

                if (fieldToSearch != null && valueToSearch != null)
                {
                    // Scroll for the last X hours only (UTC)
                    searchResponse =
                      await _client.SearchAsync<T>(sd => sd
                        .Index(indexToScroll)
                        .From(0)
                        .Take(scrollForXDocuments)
                        .Query(query =>
                          query
                            .DateRange(s => s
                              .Field(timeStampField)
                              .GreaterThanOrEquals(DateTime.UtcNow.Subtract(TimeSpan.FromDays(scrollLastXHours.Value)))))
                        .Query(query =>
                          query.Match(c => c
                            .Field(fieldToSearch)
                            .Query(valueToSearch.ToString())))
                        .Scroll(scrollTime));
                }
                else
                {
                    // Scroll for the last X hours only (UTC) with no specific search query
                    searchResponse =
                      await _client.SearchAsync<T>(sd => sd
                        .Index(indexToScroll)
                        .From(0)
                        .Take(scrollForXDocuments)
                        .Query(query =>
                          query
                            .DateRange(s => s
                              .Field(timeStampField)
                              .GreaterThanOrEquals(DateTime.UtcNow.Subtract(TimeSpan.FromDays(scrollLastXHours.Value)))))
                        .Scroll(scrollTime));
                }
            }
            else if (scrollLastXDays != null)
            {
                // Prevent attempt to scroll over a TimeSpan of zero
                if (scrollLastXDays == 0)
                {
                    scrollLastXDays = 1;
                }

                if (fieldToSearch != null && valueToSearch != null)
                {
                    // Scroll for the last X days only only (UTC)
                    searchResponse =
                      await _client.SearchAsync<T>(sd => sd
                        .Index(indexToScroll)
                        .From(0)
                        .Take(scrollForXDocuments)
                        .Query(query =>
                          query.DateRange(s => s
                            .Field(timeStampField)
                            .GreaterThanOrEquals(
                              DateTime.UtcNow.Subtract(TimeSpan.FromDays(scrollLastXDays.Value)))))
                        .Query(query =>
                          query.Match(c => c
                            .Field(fieldToSearch)
                            .Query(valueToSearch.ToString())))
                        .Scroll(scrollTime));
                }
                else
                {
                    // Scroll for the last X days only only (UTC) with no specific search query
                    searchResponse =
                      await _client.SearchAsync<T>(sd => sd
                        .Index(indexToScroll)
                        .From(0)
                        .Take(scrollForXDocuments)
                        .Query(query =>
                          query.DateRange(s => s
                            .Field(timeStampField)
                            .GreaterThanOrEquals(
                              DateTime.UtcNow.Subtract(TimeSpan.FromDays(scrollLastXDays.Value)))))
                        .Scroll(scrollTime));
                }
            }

            if (searchResponse == null || string.IsNullOrEmpty(searchResponse.ScrollId))
            {
                _logger?.LogInformation($"~ ~ ~ Floe received a null search response or failed to scroll (index may not exist)");
            }

            return searchResponse;

            #region Use this snippet as a guide for how to scroll using this method
            //bool continueScrolling = true;
            //while (continueScrolling && searchResponse != null)
            //{
            //    if (searchResponse.Documents != null && !searchResponse.IsValid)
            //    {
            //        _logger?.LogError($"~ ~ ~ Floe received an error while searching (scrolling) {searchResponse.ServerError?.Error?.Reason}");
            //        break;
            //    }

            //    if (searchResponse.Documents != null && !searchResponse.Documents.Any())
            //    {
            //        continueScrolling = false;
            //    }
            //    else
            //    {
            //        results.AddRange(searchResponse.Documents);
            //        searchResponse = await _client.ScrollAsync<T>(scrollTime, searchResponse.ScrollId);
            //    }
            //}

            //await EndSearch(searchResponse);

            //return results;
            #endregion
        }

        /// <summary>
        /// Continue a pre-existing scroll
        /// (NOTE:
        /// after you are done scrolling, you should call 'EndScroll' to clear your scroll)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="searchResponse"></param>
        /// <param name="scrollTime"></param>
        /// <returns></returns>
        public async Task<ISearchResponse<T>> ContinueScroll<T>(
            ISearchResponse<T> searchResponse,
            string scrollTime = "60s") where T : class =>
                await _client.ScrollAsync<T>(scrollTime, searchResponse.ScrollId);

        /// <summary>
        /// End a scroll by clearing the scroll
        /// (NOTE:
        /// you should call this when you are done Searching using 'BeginScroll'.
        /// You DO NOT need to call this if you are just using the 'Search' method)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="searchResponse"></param>
        public async Task EndScroll<T>(
            ISearchResponse<T> searchResponse) where T : class =>
                await _client.ClearScrollAsync(new ClearScrollRequest(searchResponse.ScrollId));

        /// <summary>
        /// (Expensive) search for documents with pagination (e.g.: in a DataGrid scenario)
        /// (NOTE:
        /// don't use this if your grid is going to have millions of records in it
        /// as this won't be performant in such a scenario)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldToSearch">The name of the field in the document to search (e.g.: "customerId" or "animal.name")</param>
        /// <param name="valueToSearch">The value to search for</param>
        /// <param name="page">The page of the DataGrid - default is 1</param>
        /// <param name="recordsOnPage">How many records each page of the DataGrid contains - default is 20</param>
        /// <param name="index">(Optional) index to search - if none provided the default index will be used</param>
        public async Task<IEnumerable<T>> SearchPaged<T>(
          string fieldToSearch,
          object valueToSearch,
          int page = 1,
          int recordsOnPage = 20,
          string index = null) where T : class
        {
            string indexToScroll = IndexToSearch(index);
            ISearchResponse<T> searchResponse = null;
            List<T> results = new List<T>();

            // Autocorrect the incorrect assumption of this method's intent
            if (page < 1)
            {
                page = 1;
            }

            // Autocorrect the incorrect assumption of this method's intent
            if (recordsOnPage < 1)
            {
                recordsOnPage = 1;
            }

            // e.g.:
            // page,recordsOnPage (1, 20) ==>
            //  from,size (0, 20)
            // page,recordsOnPage (2, 20) ==>
            //  from,size (20, 20)
            // page,recordsOnPage (3, 20) ==>
            //  from,size (40, 20)
            // etc.
            int from = (page * recordsOnPage) - recordsOnPage;
            int size = recordsOnPage;

            searchResponse =
                await _client.SearchAsync<T>(sd => sd
                .Index(indexToScroll)
                .Query(q =>
                    q.Match(c => c
                    .Field(fieldToSearch)
                    .Query(valueToSearch.ToString())))
                .From(page)
                .Size(recordsOnPage));

            if (searchResponse == null || !searchResponse.Documents.Any())
            {
                _logger?.LogInformation($"~ ~ ~ Floe received a null search response or failed to scroll (index may not exist)");
                return results;
            }

            results.AddRange(searchResponse.Documents);

            return results;
        }

        /// <summary>
        /// Count all the documents in an index that match a given search query.
        /// Leave fieldToSearch and valueToSearch null if you want to count all documents in an index
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldToSearch">(Optional) the name of the field in the document to search (e.g.: "customerId" or "animal.name")</param>
        /// <param name="valueToSearch">(Optional) the value to search for</param>
        /// <param name="index">(Optional) index to count - if none provided the default index will be used</param>
        public async Task<long> Count<T>(
          string fieldToSearch = null,
          object valueToSearch = null,
          string index = null) where T : class
        {
            string indexToCount = IndexToSearch(index);

            try
            {
                CountResponse countResponse;

                // 'CountBySearch'
                if (fieldToSearch != null && valueToSearch != null)
                {
                    countResponse =
                      await _client.CountAsync<T>(c => c
                        .Index(indexToCount)
                        .Query(q => q
                          .Match(m => m
                            .Field(fieldToSearch)
                            .Query(valueToSearch.ToString()))));
                }
                else
                {
                    countResponse =
                        await _client.CountAsync<T>(c => c
                        .Index(indexToCount));
                }

                return countResponse.Count;
            }
            catch (Exception exception)
            {
                string exceptionLogPrefix = $"~ ~ ~ Floe threw an exception while trying to count documents in index {indexToCount}";

                string errorMessage =
                  $"{exceptionLogPrefix}{Environment.NewLine}{JsonConvert.SerializeObject(exception)}";

                _logger?.LogError(errorMessage);

                // ReSharper disable once PossibleIntendedRethrow
                throw exception;
            }
        }

        /// <summary>
        /// Write the document to an Elasticsearch index. Uses BulkAsync and 'numberOfBulkDocumentsToWriteAtOnce'
        /// </summary>
        /// <typeparam name="T">Index POCO</typeparam>
        /// <param name="document">Document to write to the Elasticsearch index</param>
        /// <param name="allowDuplicates">(Optional) if true the documents about to be bulk written will not be validated for duplicates (side-effect of async operations) - default is false</param>
        /// <param name="index">(Optional) index to write to - if none provided the default index will be used</param>
        public async Task Write<T>(
          T document,
          bool allowDuplicates = false,
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
                      .IndexMany(!allowDuplicates ? _documents.Distinct() : _documents)
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

                    _documents.Clear();
                }
            }
            catch (Exception exception)
            {
                string exceptionLogPrefix = $"~ ~ ~ Floe threw an exception while trying to write to index {indexToWriteTo}";

                string errorMessage =
                  $"{exceptionLogPrefix}{Environment.NewLine}{JsonConvert.SerializeObject(exception)}";

                _logger?.LogError(errorMessage);

                // ReSharper disable once PossibleIntendedRethrow
                throw exception;
            }
        }

        /// <summary>
        /// Write any remaining documents queue. If calling Write many times, you may want to call this method once at the end to 'flush'
        /// any unwritten documents from the queue (e.g.: if your numberOfBulkDocumentsToWriteAtOnce is 5, and you last wrote 4 documents, calling this method
        /// will write the final document)
        /// </summary>
        /// <typeparam name="T">Index POCO</typeparam>
        /// <param name="index">(Optional) index to write to - if none provided the default index will be used</param>
        public async Task WriteUnwritten<T>(
          bool allowDuplicates = false,
          string index = null)
        {
            string indexToWriteTo = IndexToWriteTo(index);

            try
            {
                BulkDescriptor bulkDescriptor = new BulkDescriptor();
                bulkDescriptor
                  .IndexMany(!allowDuplicates ? _documents.Distinct() : _documents)
                  .Index(indexToWriteTo);

                BulkResponse bulkResponse = await _client.BulkAsync(bulkDescriptor);

                if (bulkResponse.Errors)
                {
                    string errorLogPrefix = $"~ ~ ~ Floe received an error while trying to write all unwritten documents to index {indexToWriteTo}";

                    string errorMessage =
                      $"{errorLogPrefix}{Environment.NewLine}{JsonConvert.SerializeObject(bulkResponse.Errors)}";

                    _logger?.LogError(errorMessage);

                    throw new Exception(errorMessage);
                }

                _documents.Clear();
            }
            catch (Exception exception)
            {
                string exceptionLogPrefix = $"~ ~ ~ Floe threw an exception while trying to write all unwritten documents to index {indexToWriteTo}";

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
        /// Delete all indices (does not delete system indices)
        /// </summary>
        /// <returns>True if all indices were successfully deleted</returns>
        public async Task<bool> DeleteAllIndices()
        {
            _logger?.LogInformation($"~ ~ ~ Floe is deleting all indices");

            List<bool> indexDeletions = new List<bool>();
            foreach (KeyValuePair<IndexName, IndexState> index
            in (await _client.Indices.GetAsync(new GetIndexRequest(Indices.All))).Indices)
            {
                // Do not delete system indices
                if (!index.Key.Name.StartsWith('.'))
                {
                    indexDeletions.Add(await DeleteIndex(index.Key.Name));
                }
            }

            return indexDeletions.All(indexDeletion => true);
        }

        /// <summary>
        /// Delete an index (does not delete system indices)
        /// </summary>
        /// <param name="index">Index name</param>
        public async Task<bool> DeleteIndex(string index)
        {
            // Do not delete system indices
            if (!string.IsNullOrEmpty(index) && !index.StartsWith('.'))
            {
                _logger?.LogInformation($"~ ~ ~ Floe is deleting index {index}");

                return (await _client.Indices.DeleteAsync(index)).IsValid;
            }

            return false;
        }

        #region Disposable implementation
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            _disposed = true;
        }

        public virtual ValueTask DisposeAsync()
        {
            try
            {
                Dispose();
                return default;
            }
            catch (Exception exception)
            {
                return new ValueTask(Task.FromException(exception));
            }
        }
        #endregion
    }
}
