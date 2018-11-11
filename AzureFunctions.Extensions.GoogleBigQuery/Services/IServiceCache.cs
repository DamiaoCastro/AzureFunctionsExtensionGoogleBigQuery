namespace AzureFunctions.Extensions.GoogleBigQuery.Services {
    public interface IServiceCache {

        T GetFromCache<T>(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute) where T : class;

        void SaveToCache<T>(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute, T service, int minutesToCache) where T : class;

    }
}