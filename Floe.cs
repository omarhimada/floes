﻿using Amazon.Extensions.NETCore.Setup;
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
        /// Build the 'Index' string for the BulkRequest
        /// </summary>
        private string IndexToWriteTo(string index = null)
        {
            string prefix = string.IsNullOrEmpty(index) ? _defaultIndex : index;
            string suffix = _rollingDate ? $"{DateTime.UtcNow:yyyy.MM.dd}-" : string.Empty;
            return $"{prefix}{suffix}";
        }

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
        /// <param name="scrollTime">(Optional) TTL of the scroll until another List is called - default is 60s</param>
        /// <param name="index">(Optional) index to scroll - if none provided the default index will be used</param>
        public async Task<IEnumerable<T>> List<T>(
          string scrollTime = "60s",
          string index = null) where T : class
        {
            string indexToScroll = IndexToWriteTo(index);

            ISearchResponse<T> searchResponse =
              await _client.SearchAsync<T>(sd => sd
                .Index(indexToScroll)
                .From(0)
                .Take(1000)
                .MatchAll()
                .Scroll(scrollTime));

            List<T> results = new List<T>();

            bool continueScrolling = true;
            while (continueScrolling)
            {
                if (!searchResponse.IsValid || string.IsNullOrEmpty(searchResponse.ScrollId))
                {
                    _logger?.LogError($"Search error: {searchResponse.ServerError.Error.Reason}");
                }

                if (!searchResponse.Documents.Any())
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
        /// Write the document to an Elasticsearch index. Uses BulkRequests
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
                if (_documents.Count > _numberOfBulkDocumentsToWriteAtOnce)
                {
                    BulkDescriptor bulkDescriptor = new BulkDescriptor();
                    bulkDescriptor
                      .IndexMany(_documents)
                      .Index(indexToWriteTo);

                    BulkResponse bulkResponse = await _client.BulkAsync(bulkDescriptor);

                    if (bulkResponse.Errors)
                    {
                        string errorLogPrefix = $"Floe Write received an error while trying to write to index {indexToWriteTo}";

                        _logger?.LogError(
                            $"{errorLogPrefix}{Environment.NewLine}{JsonConvert.SerializeObject(bulkResponse.Errors)}");
                    }
                    else
                    {
                        _documents.Clear();
                    }
                }
            }
            catch (Exception exception)
            {
                string exceptionLogPrefix = $"Floe Write threw an exception while trying to write to index {indexToWriteTo}";

                _logger?.LogError(
                    $"{exceptionLogPrefix}{Environment.NewLine}{exception.Message}{Environment.NewLine}{exception.StackTrace}");
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
                return response.Source;
            }

            _logger?.LogError($"Floe Find could not find document of type {typeof(T).Name} with ID {id}");

            return null;
        }
    }
}