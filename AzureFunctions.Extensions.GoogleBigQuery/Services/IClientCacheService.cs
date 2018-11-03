using TransparentApiClient.Google.BigQuery.V2.Resources;

namespace AzureFunctions.Extensions.GoogleBigQuery.Services {
    public interface IClientCacheService<T> {

        T GetClient(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute);

    }
}