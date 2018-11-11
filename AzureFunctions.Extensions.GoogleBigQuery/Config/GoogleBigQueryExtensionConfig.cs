using AzureFunctions.Extensions.GoogleBigQuery.Bindings;
using AzureFunctions.Extensions.GoogleBigQuery.Services;
using Microsoft.Azure.WebJobs.Description;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.WebJobs.Host.Config;
using System;
using System.Threading.Tasks;
using TransparentApiClient.Google.BigQuery.V2.Resources;

namespace AzureFunctions.Extensions.GoogleBigQuery.Config {

    [Extension("GoogleBigQuery", "GoogleBigQuery")]
    public partial class GoogleBigQueryExtensionConfig : IExtensionConfigProvider {

        private readonly static IServiceCache serviceCache = new ServiceCache();
        private readonly static IServiceFactory serviceFactory = new ServiceFactory(serviceCache);

        void IExtensionConfigProvider.Initialize(ExtensionConfigContext context) {
            if (context == null) { throw new ArgumentNullException(nameof(context)); }

            context
                .AddBindingRule<GoogleBigQueryCollectorAttribute>()
                .BindToCollector(GetGoogleBigQueryAsyncCollector);

            context
                .AddBindingRule<GoogleBigQueryResourceAttribute>()
                .BindToInput(GetGoogleBigQueryJobInput<IDatasets>);

            context
                .AddBindingRule<GoogleBigQueryResourceAttribute>()
                .BindToInput(GetGoogleBigQueryJobInput<IJobs>);

            context
                .AddBindingRule<GoogleBigQueryResourceAttribute>()
                .BindToInput(GetGoogleBigQueryJobInput<IProjects>);

            context
                .AddBindingRule<GoogleBigQueryResourceAttribute>()
                .BindToInput(GetGoogleBigQueryJobInput<ITabledata>);

            context
                .AddBindingRule<GoogleBigQueryResourceAttribute>()
                .BindToInput(GetGoogleBigQueryJobInput<ITables>);

        }

        private GoogleBigQueryAsyncCollector GetGoogleBigQueryAsyncCollector(GoogleBigQueryCollectorAttribute googleBigQueryCollectorAttribute) {

            var bigQueryService = serviceFactory.GetService<BigQueryService>(googleBigQueryCollectorAttribute);
            var service = new GoogleBigQueryAsyncCollector(bigQueryService);

            return service;
        }

        private Task<T> GetGoogleBigQueryJobInput<T>(GoogleBigQueryResourceAttribute googleBigQueryResourceAttribute, ValueBindingContext valueBindingContext) where T : class {

            var service = serviceFactory.GetService<T>(googleBigQueryResourceAttribute);
            
            return Task.FromResult<T>(service);
        }

    }
}