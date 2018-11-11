namespace AzureFunctions.Extensions.GoogleBigQuery.Services {
    public interface IServiceFactory {

        T GetService<T>(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute) where T : class;

    }
}