using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host.Config;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AzureFunctions.Extensions.GoogleBigQuery
{

    public partial class GoogleBigQueryExtensionConfig : IExtensionConfigProvider
    {

        void IExtensionConfigProvider.Initialize(ExtensionConfigContext context)
        {
            if (context == null) { throw new ArgumentNullException(nameof(context)); }

            context.AddBindingRule<GoogleBigQueryAttribute>()
                .BindToCollector(c => new AsyncCollector(c));

            context.AddBindingRule<GoogleBigQueryManagementAttribute>()
                .BindToInput(c => new GoogleBigQueryManagement(c));

        }
        
    }

}