using Microsoft.Azure.WebJobs.Host.Config;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    public partial class GoogleBigQueryExtensionConfig : IExtensionConfigProvider {

        void IExtensionConfigProvider.Initialize(ExtensionConfigContext context) {
            if (context == null) { throw new ArgumentNullException(nameof(context)); }

            context
                .AddBindingRule<GoogleBigQueryAttribute>()
                .BindToCollector(c => new AsyncCollector(c));

        }

    }

}