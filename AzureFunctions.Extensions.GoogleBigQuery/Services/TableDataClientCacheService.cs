using System;
using System.Collections.Concurrent;
using TransparentApiClient.Google.BigQuery.V2.Resources;
using bq = TransparentApiClient.Google.BigQuery.V2.Resources;

namespace AzureFunctions.Extensions.GoogleBigQuery.Services {
    public class TableDataClientCacheService : ITableDataClientCacheService {

        bq.ITabledata ITableDataClientCacheService.GetTabledataClient(GoogleBigQueryAttribute googleBigQueryAttribute) {
            var key = $"{googleBigQueryAttribute.GetHashCode()}".GetHashCode();

            if (tableDataClientCache.ContainsKey(key)) {
                var expiringService = tableDataClientCache[key];
                if ((DateTime.UtcNow - expiringService.CreatedUtc).TotalHours > 1) {
                    return GenerateAndSaveNewService(googleBigQueryAttribute, key);
                }

                return expiringService.Service;
            } else {
                return GenerateAndSaveNewService(googleBigQueryAttribute, key);
            }
        }

        private static ConcurrentDictionary<int, ExpiringService<bq.ITabledata>> tableDataClientCache = new ConcurrentDictionary<int, ExpiringService<bq.ITabledata>>();

        private ITabledata GenerateAndSaveNewService(GoogleBigQueryAttribute googleBigQueryAttribute, int key) {
            var credentials = GetCredentials(googleBigQueryAttribute);
            bq.ITabledata tableData = new Tabledata(credentials);
            var expiringService1 = new ExpiringService<bq.ITabledata>(DateTime.UtcNow, tableData);
            tableDataClientCache.AddOrUpdate(key, expiringService1, (newkey, oldValue) => expiringService1);

            return tableData;
        }

        private byte[] GetCredentials(GoogleBigQueryAttribute googleBigQueryAttribute) {

            byte[] googleCredential = null;
            if (googleBigQueryAttribute.Credentials != null) {
                googleCredential = googleBigQueryAttribute.Credentials;
            } else {
                if (!string.IsNullOrWhiteSpace(googleBigQueryAttribute.CredentialsFileName)) {
                    var path = System.IO.Path.GetDirectoryName(typeof(GoogleBigQueryAttribute).Assembly.Location);
                    var fullPath = System.IO.Path.Combine(path, "..", googleBigQueryAttribute.CredentialsFileName);
                    var credentials = System.IO.File.ReadAllBytes(fullPath);
                    googleCredential = credentials;
                }
            }

            return googleCredential;
        }

        private class ExpiringService<T> {

            public ExpiringService(DateTime createdUtc, T service) {
                CreatedUtc = createdUtc;
                Service = service;
            }

            public DateTime CreatedUtc { get; }
            public T Service { get; }

        }

    }
}