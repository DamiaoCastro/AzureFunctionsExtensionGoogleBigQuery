using AzureFunctions.Extensions.GoogleBigQuery.Config;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Hosting;

[assembly: WebJobsStartup(typeof(GoogleBigQueryWebJobStartup), "GoogleBigQuery")]
namespace AzureFunctions.Extensions.GoogleBigQuery.Config {

    class GoogleBigQueryWebJobStartup : IWebJobsStartup {
        void IWebJobsStartup.Configure(IWebJobsBuilder builder) {
            builder.UseGoogleBigQuery();
        }
    }
}