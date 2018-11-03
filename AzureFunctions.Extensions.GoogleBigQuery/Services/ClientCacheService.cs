using System;
using System.Collections.Concurrent;
using TransparentApiClient.Google.BigQuery.V2.Resources;

namespace AzureFunctions.Extensions.GoogleBigQuery.Services {
    internal class ClientCacheService<T> : IClientCacheService<T> where T : class {

        T IClientCacheService<T>.GetClient(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute) {
            var key = $"{googleBigQueryBaseAttribute.GetHashCode()}".GetHashCode();

            if (clientCache.ContainsKey(key)) {
                var expiringService = clientCache[key];
                if ((DateTime.UtcNow - expiringService.CreatedUtc).TotalHours > 1) {
                    return GenerateAndSaveNewService(googleBigQueryBaseAttribute, key);
                }

                return expiringService.Service;
            } else {
                return GenerateAndSaveNewService(googleBigQueryBaseAttribute, key);
            }
        }

        private static ConcurrentDictionary<int, ExpiringService<T>> clientCache = new ConcurrentDictionary<int, ExpiringService<T>>();

        private T GenerateAndSaveNewService(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute, int key) {

            byte[] credentials = GetCredentials(googleBigQueryBaseAttribute);

            T service = null;
            if (typeof(T) == typeof(IDatasets)) { service = (T)(IDatasets)new Datasets(credentials); }
            if (typeof(T) == typeof(IJobs)) { service = (T)(IJobs)new Jobs(credentials); }
            if (typeof(T) == typeof(IProjects)) { service = (T)(IProjects)new Projects(credentials); }
            if (typeof(T) == typeof(ITabledata)) { service = (T)(ITabledata)new Tabledata(credentials); }
            if (typeof(T) == typeof(ITables)) { service = (T)(ITables)new Tables(credentials); }

            if (service is null) { throw new NotImplementedException(); }

            var expiringService1 = new ExpiringService<T>(DateTime.UtcNow, service);
            clientCache.AddOrUpdate(key, expiringService1, (newkey, oldValue) => expiringService1);

            return service;
        }

        private byte[] GetCredentials(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute) {

            var credentials = Environment.GetEnvironmentVariable(googleBigQueryBaseAttribute.CredentialsSettingKey, EnvironmentVariableTarget.Process);
            if (string.IsNullOrWhiteSpace(credentials)) {
                throw new MissingSettingException($"There's no value for the setting key: '{googleBigQueryBaseAttribute.CredentialsSettingKey}'.");
            }

            return System.Text.Encoding.UTF8.GetBytes(credentials);
        }

        private class ExpiringService<X> {

            public ExpiringService(DateTime createdUtc, X service) {
                CreatedUtc = createdUtc;
                Service = service;
            }

            public DateTime CreatedUtc { get; }
            public X Service { get; }

        }

    }
}