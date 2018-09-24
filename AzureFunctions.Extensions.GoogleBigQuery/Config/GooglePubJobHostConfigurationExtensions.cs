using Microsoft.Azure.WebJobs;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery.Config {

    public static class GooglePubJobHostConfigurationExtensions {

        /// <summary>
        /// Enables use of the Google PubSub extensions for webjobs
        /// </summary>
        /// <param name="config">The <see cref="JobHostConfiguration"/> to configure.</param>
        public static void UseGoogleBigQuery(this IWebJobsBuilder config) {

            if (config == null) {
                throw new ArgumentNullException(nameof(config));
            }

            // Register our extension configuration provider
            config.AddExtension<GoogleBigQueryExtensionConfig>();

        }

    }
}