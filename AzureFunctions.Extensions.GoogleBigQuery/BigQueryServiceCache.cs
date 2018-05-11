using System;
using System.Collections.Concurrent;

namespace AzureFunctions.Extensions.GoogleBigQuery
{
    internal class BigQueryServiceCache
    {

        private static ConcurrentDictionary<int, ExpiringBigQueryService> publisherClientCache = new ConcurrentDictionary<int, ExpiringBigQueryService>();

        public static BigQueryService GetPublisherClient(GoogleBigQueryAttribute googleBigQueryAttribute/*, Type itemType*/)
        {
            var key = $"{googleBigQueryAttribute.GetHashCode()}".GetHashCode();

            if (publisherClientCache.ContainsKey(key))
            {
                var expiringBigQueryService = publisherClientCache[key];
                if ((DateTime.UtcNow - expiringBigQueryService.CreatedUtc).TotalHours > 1) {
                    var bigQueryService = new BigQueryService(googleBigQueryAttribute);
                    var expiringBigQueryService1 = new ExpiringBigQueryService(DateTime.UtcNow, bigQueryService);
                    publisherClientCache.AddOrUpdate(key, expiringBigQueryService1, (newkey, oldValue) => expiringBigQueryService1);

                    return bigQueryService;
                }

                return expiringBigQueryService.BigQueryService;
            }
            else
            {
                var bigQueryService = new BigQueryService(googleBigQueryAttribute/*, itemType*/);
                var expiringBigQueryService = new ExpiringBigQueryService(DateTime.UtcNow, bigQueryService);
                publisherClientCache.AddOrUpdate(key, expiringBigQueryService, (newkey, oldValue) => expiringBigQueryService);

                return bigQueryService;
            }

        }

        private class ExpiringBigQueryService
        {

            public ExpiringBigQueryService(DateTime createdUtc, BigQueryService bigQueryService)
            {
                CreatedUtc = createdUtc;
                BigQueryService = bigQueryService;
            }

            public DateTime CreatedUtc { get; }
            public BigQueryService BigQueryService { get; }

        }

    }
}
