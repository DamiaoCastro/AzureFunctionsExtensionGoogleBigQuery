using AzureFunctions.Extensions.GoogleBigQuery.Bindings;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.WebJobs.Description;
using Microsoft.Azure.WebJobs.Host.Config;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery.Config {

    [Extension("GoogleBigQuery", "GoogleBigQuery")]
    public partial class GoogleBigQueryExtensionConfig : IExtensionConfigProvider {

        private readonly static GoogleCredential credentials = GoogleCredential.GetApplicationDefault();
        private readonly static ServiceAccountCredential serviceAccountCredential = credentials.UnderlyingCredential as ServiceAccountCredential;
        private readonly static BigQueryClient bigqueryClient = BigQueryClient.Create(serviceAccountCredential.ProjectId, credentials);

        void IExtensionConfigProvider.Initialize(ExtensionConfigContext context) {
            if (context == null) { throw new ArgumentNullException(nameof(context)); }

            context
                .AddBindingRule<GoogleBigQueryCollectorAttribute>()
                .BindToCollector(GetGoogleBigQueryAsyncCollector);

        }

        private GoogleBigQueryAsyncCollector GetGoogleBigQueryAsyncCollector(GoogleBigQueryCollectorAttribute googleBigQueryCollectorAttribute) {
            return new GoogleBigQueryAsyncCollector(googleBigQueryCollectorAttribute, bigqueryClient);
        }

    }
}