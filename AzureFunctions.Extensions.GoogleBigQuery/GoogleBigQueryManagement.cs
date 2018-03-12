using System;
using System.Threading;
using System.Threading.Tasks;

namespace AzureFunctions.Extensions.GoogleBigQuery {
    public class GoogleBigQueryManagement {
        private readonly GoogleBigQueryManagementAttribute googleBigQueryManagementAttribute;

        public GoogleBigQueryManagement(GoogleBigQueryManagementAttribute googleBigQueryManagementAttribute) {
            this.googleBigQueryManagementAttribute = googleBigQueryManagementAttribute;
        }

        public Task DeleteTableAsync(string tableName, CancellationToken cancellationToken) {
            //var service = new BigQueryService(googleBigQueryManagementAttribute, null);
            //return service.DeleteTableAsync(tableName, cancellationToken);

            throw new NotImplementedException();
        }

        public Task CreateTableAsync<T>(bool timePartitioning, CancellationToken cancellationToken) {
            //var service = new BigQueryService(googleBigQueryManagementAttribute, typeof(T));
            //return service.CreateTableAsync(timePartitioning, cancellationToken);

            throw new NotImplementedException();
        }

    }
}