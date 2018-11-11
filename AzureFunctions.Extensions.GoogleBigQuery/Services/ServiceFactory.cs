using System;
using TransparentApiClient.Google.BigQuery.V2.Resources;

namespace AzureFunctions.Extensions.GoogleBigQuery.Services {
    public sealed class ServiceFactory : IServiceFactory {

        private readonly IServiceCache serviceCache;

        public ServiceFactory(IServiceCache serviceCache) {
            this.serviceCache = serviceCache;
        }

        T IServiceFactory.GetService<T>(GoogleBigQueryBaseAttribute googleBigQueryBaseAttribute) {

            T service = serviceCache.GetFromCache<T>(googleBigQueryBaseAttribute);
            if (service is null) {

                switch (googleBigQueryBaseAttribute) {
                    case GoogleBigQueryCollectorAttribute googleBigQueryCollectorAttribute:

                        if (typeof(T) != typeof(IBigQueryService)) { throw new NotImplementedException(); }

                        service = (T)GetBigQueryService(googleBigQueryCollectorAttribute);
                        break;
                    case GoogleBigQueryResourceAttribute googleBigQueryResourceAttribute:
                        service = CreateResourceService<T>(googleBigQueryResourceAttribute);
                        break;
                    default:
                        throw new NotImplementedException();
                }

                serviceCache.SaveToCache<T>(googleBigQueryBaseAttribute, service, 10);
            }

            return service;
        }

        private IBigQueryService GetBigQueryService(GoogleBigQueryCollectorAttribute googleBigQueryCollectorAttribute) {
            ITabledata tableData = ((IServiceFactory)this).GetService<ITabledata>(new GoogleBigQueryResourceAttribute(googleBigQueryCollectorAttribute.CredentialsSettingKey));
            return new BigQueryService(googleBigQueryCollectorAttribute, tableData);
        }

        private T CreateResourceService<T>(GoogleBigQueryResourceAttribute googleBigQueryResourceAttribute) where T : class {

            byte[] credentials = googleBigQueryResourceAttribute.Credentials;

            if (typeof(T) == typeof(IDatasets)) { return (T)(IDatasets)new Datasets(credentials); }
            if (typeof(T) == typeof(IJobs)) { return (T)(IJobs)new Jobs(credentials); }
            if (typeof(T) == typeof(IProjects)) { return (T)(IProjects)new Projects(credentials); }
            if (typeof(T) == typeof(ITabledata)) { return (T)(ITabledata)new Tabledata(credentials); }
            if (typeof(T) == typeof(ITables)) { return (T)(ITables)new Tables(credentials); }

            throw new NotImplementedException();
        }

    }
}