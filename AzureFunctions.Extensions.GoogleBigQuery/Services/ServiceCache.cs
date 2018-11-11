using System;
using System.Collections.Concurrent;

namespace AzureFunctions.Extensions.GoogleBigQuery.Services {
    public sealed class ServiceCache : IServiceCache {

        private static ConcurrentDictionary<string, ExpiringService> clientCache = new ConcurrentDictionary<string, ExpiringService>();

        T IServiceCache.GetFromCache<T>(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute) {
            if (googleBigQueryBaseAttribute == null) { throw new ArgumentNullException(nameof(googleBigQueryBaseAttribute)); }

            var key = GetKey<T>(googleBigQueryBaseAttribute);

            if (clientCache.ContainsKey(key)) {
                var expiringService = clientCache[key];
                if ((DateTime.UtcNow - expiringService.CreatedUtc).TotalMinutes > expiringService.MinutesToCache) {
                    return null;
                }

                return (T)expiringService.Service;
            }

            return null;
        }

        void IServiceCache.SaveToCache<T>(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute, T service, int minutesToCache) {
            if (googleBigQueryBaseAttribute == null) { throw new ArgumentNullException(nameof(googleBigQueryBaseAttribute)); }
            if (service == null) { throw new ArgumentNullException(nameof(service)); }
            if (minutesToCache < 0) { throw new ArgumentOutOfRangeException(nameof(minutesToCache), $"The parameter '{nameof(minutesToCache)}' must be positive"); }

            var key = GetKey<T>(googleBigQueryBaseAttribute);
            var expiringService = new ExpiringService(DateTime.UtcNow, service, minutesToCache);
            clientCache.AddOrUpdate(key, expiringService, (newkey, oldValue) => expiringService);

        }

        private string GetKey<T>(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute) {
            return $"{typeof(T)}|{googleBigQueryBaseAttribute.GetObjectKey()}"; ;
        }

        private class ExpiringService {

            public ExpiringService(DateTime createdUtc, object service, int minutesToCache) {
                CreatedUtc = createdUtc;
                Service = service;
                MinutesToCache = minutesToCache;
            }

            public DateTime CreatedUtc { get; }
            public object Service { get; }
            public double MinutesToCache { get; }

        }

    }
}