using AzureFunctions.Extensions.GoogleBigQuery.Bindings;
using AzureFunctions.Extensions.GoogleBigQuery.Services;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Description;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.WebJobs.Host.Config;
using System;
using System.Threading.Tasks;
using TransparentApiClient.Google.BigQuery.V2.Resources;

namespace AzureFunctions.Extensions.GoogleBigQuery.Config {

    [Extension("GoogleBigQuery", "GoogleBigQuery")]
    public partial class GoogleBigQueryExtensionConfig : IExtensionConfigProvider {

        void IExtensionConfigProvider.Initialize(ExtensionConfigContext context) {
            if (context == null) { throw new ArgumentNullException(nameof(context)); }

            context
                .AddBindingRule<GoogleBigQueryAttribute>()
                .BindToCollector(c => {

                    ITableDataClientCacheService tableDataClientCacheService = new TableDataClientCacheService();
                    var bigQueryService = new BigQueryService(c, tableDataClientCacheService);

                    return new GoogleBigQueryAsyncCollector(bigQueryService);
                });

            //context
            //    .AddBindingRule<GoogleBigQueryJobAttribute>()
            //    .BindToInput(GetGoogleBigQueryJobInput<IDatasets>);

            //context
            //    .AddBindingRule<GoogleBigQueryJobAttribute>()
            //    .BindToInput(GetGoogleBigQueryJobInput<IJobs>);

            //context
            //    .AddBindingRule<GoogleBigQueryJobAttribute>()
            //    .BindToInput(GetGoogleBigQueryJobInput<IProjects>);

            //context
            //    .AddBindingRule<GoogleBigQueryJobAttribute>()
            //    .BindToInput(GetGoogleBigQueryJobInput<ITabledata>);

            //context
            //    .AddBindingRule<GoogleBigQueryJobAttribute>()
            //    .BindToInput(GetGoogleBigQueryJobInput<ITables>);

        }

        private GoogleBigQueryAsyncCollector GetGoogleBigQueryAsyncCollector(GoogleBigQueryAttribute googleBigQueryAttribute) {

            ITableDataClientCacheService tableDataClientCacheService = new TableDataClientCacheService();
            var bigQueryService = new BigQueryService(googleBigQueryAttribute, tableDataClientCacheService);

            return new GoogleBigQueryAsyncCollector(bigQueryService);
        }

        //private Task<T> GetGoogleBigQueryJobInput<T>(GoogleBigQueryJobAttribute attribute, ValueBindingContext valueBindingContext) where T : class {

        //    IClientCacheService<T> tableDataClientCacheService = new ClientCacheService<T>();
        //    var service = tableDataClientCacheService.GetClient(attribute);

        //    return Task.FromResult<T>(service);
        //}

    }
}