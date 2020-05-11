using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace FloES
{
    /// <summary>
    /// Extend Floe to include IMemoryCache handling
    /// </summary>
    public partial class Floe<T> where T : class
    {
        /// <summary>
        /// Any time the Floe writes to IMemoryCache the associated cache key is appended to this list
        /// </summary>
        protected List<string> _cacheKeys = new List<string>();

        /// <summary>
        /// Default sliding expiration setting for caching Elasticsearch data
        /// TODO: make this configurable
        /// </summary>
        private static MemoryCacheEntryOptions _cacheEntryOptions =
            new MemoryCacheEntryOptions()
                // Keep in cache for this time, reset time if accessed.
                .SetSlidingExpiration(TimeSpan.FromMinutes(15));

        private bool CacheKeyExists(string cacheKey) => _cacheKeys.Any(ck => string.Equals(ck, cacheKey));

        private void AppendCacheKey(string cacheKey) => _cacheKeys.Append($"{typeof(T).FullName}-{cacheKey}");

        private void RemoveCacheKey(string cacheKey) => _cacheKeys.Remove($"{typeof(T).FullName}-{cacheKey}");

        /// <summary>
        /// All IMemoryCache entires will be prefixed with this string
        /// </summary>
        private const string _memoryCachePrefix = "floe-cache";

        /// <summary>
        /// Generate the cache key using the calling method name and the parameters
        /// </summary>
        /// <param name="fieldToSearch">From caller</param>
        /// <param name="valueToSearch">From caller</param>
        /// <param name="filters">From caller</param>
        /// <param name="sort">From caller</param>
        /// <param name="xHours">From caller</param>
        /// <param name="xDays">From caller</param>
        /// <param name="scrollTime">From caller</param>
        /// <param name="index">From caller</param>
        /// <param name="timeStampField">From caller</param>
        /// <param name="caller">The name of the calling method</param>
        private string CacheKeyFromParameters(
            string fieldToSearch = null,
            object valueToSearch = null,
            (string, string)[] filters = default,
            (string, string)? sort = default,
            double? xHours = null,
            double? xDays = null,
            string scrollTime = null,
            string index = null,
            string timeStampField = null,
            int? page = null,
            int? recordsOnPage = null,
            [CallerMemberName] string caller = null)
            => $"{caller}" +
                $"{fieldToSearch ?? string.Empty}" +
                $"{valueToSearch ?? string.Empty}" +
                $"{filters?.ToString() ?? string.Empty}" +
                $"{sort?.ToString() ?? string.Empty}" +
                $"{xHours?.ToString() ?? string.Empty}" +
                $"{xDays?.ToString() ?? string.Empty}" +
                $"{scrollTime ?? string.Empty}" +
                $"{index ?? string.Empty}" +
                $"{timeStampField ?? string.Empty}" +
                $"{page?.ToString() ?? string.Empty}" +
                $"{recordsOnPage?.ToString() ?? string.Empty}";

        /// <summary>
        /// Persist some data in the IMemoryCache 
        /// </summary>
        /// <param name="memoryCache"></param>
        /// <param name="floe"></param>
        /// <param name="cacheKey"></param>
        /// <param name="data"></param>
        private void CacheElasticsearchData(
            IMemoryCache memoryCache,
            dynamic data,
            string cacheKey = null)
        {
            // If no cache key was provided just generate a new Guid
            if (cacheKey == null) cacheKey = (Guid.NewGuid()).ToString();

            // Keep track of the key used to write this data to the IMemoryCache
            AppendCacheKey(cacheKey);

            // Write the data to the IMemoryCache
            memoryCache?.Set<object>($"{_memoryCachePrefix}-{typeof(T).FullName}-{cacheKey}", (object)data);
        }

        /// <summary>
        /// Get the cached data using its key
        /// </summary>
        /// <param name="memoryCache"></param>
        /// <param name="cacheKey"></param>
        private dynamic RetrieveCachedElasticsearchData(
            IMemoryCache memoryCache,
            string cacheKey)
                => memoryCache?.Get<object>($"{_memoryCachePrefix}-{typeof(T).FullName}-{cacheKey}");

        /// <summary>
        /// Clear cached Elasticsearch data from your IMemoryCache
        /// </summary>
        /// <typeparam name="T">The document type of the cached data that will be cleared</typeparam>
        /// <param name="memoryCache">Your IMemoryCache implementation (https://docs.microsoft.com/en-us/dotnet/api/microsoft.extensions.caching.memory.imemorycache)</param>
        /// <param name="floe">The Floe that wrote to the IMemoryCache initially</param>
        /// <param name="cacheKey">(Optional) cache key of specific data set - leave null to clear all for the document type</param>
        private void ClearCachedElasticsearchData(
          IMemoryCache memoryCache,
          string cacheKey = null)
        {
            void __clearCacheForKey(string ck)
            {
                // Cache ID was provided - clear specific entry
                memoryCache?.Remove($"{_memoryCachePrefix}-{typeof(T).FullName}-{ck}");

                // Remove cache key from Floe
                RemoveCacheKey(ck);
            };

            if (!string.IsNullOrEmpty(cacheKey) &&
                CacheKeyExists(cacheKey))
            {
                __clearCacheForKey(cacheKey);
            }
            else
            {
                foreach (string ck in _cacheKeys)
                {
                    __clearCacheForKey(ck);
                }
            }
        }

        /// <summary>
        /// Clear all cached Elasticsearch data that this Floe has persisted
        /// </summary>
        /// <param name="memoryCache">Your IMemoryCache implementation (https://docs.microsoft.com/en-us/dotnet/api/microsoft.extensions.caching.memory.imemorycache)</param>
        public void FlushCachedData(IMemoryCache memoryCache) => ClearCachedElasticsearchData(memoryCache, null);
    }
}
