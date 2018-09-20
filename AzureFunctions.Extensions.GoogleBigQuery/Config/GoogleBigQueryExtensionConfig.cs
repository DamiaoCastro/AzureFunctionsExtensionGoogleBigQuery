using AzureFunctions.Extensions.GoogleBigQuery.Bindings;
using AzureFunctions.Extensions.GoogleBigQuery.Services;
using Microsoft.Azure.WebJobs.Description;
using Microsoft.Azure.WebJobs.Host.Config;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery.Config {

    [Extension("GoogleBigQuery")]
    public partial class GoogleBigQueryExtensionConfig : IExtensionConfigProvider {

        void IExtensionConfigProvider.Initialize(ExtensionConfigContext context) {
            if (context == null) { throw new ArgumentNullException(nameof(context)); }

            context
                .AddBindingRule<GoogleBigQueryAttribute>()
                .BindToCollector(c=> GetGoogleBigQueryAsyncCollector(c));

        }

        private GoogleBigQueryAsyncCollector GetGoogleBigQueryAsyncCollector(GoogleBigQueryAttribute googleBigQueryAttribute) {
            var bigQueryService = new BigQueryService(googleBigQueryAttribute);
            return new GoogleBigQueryAsyncCollector(bigQueryService);
        }

    }
}