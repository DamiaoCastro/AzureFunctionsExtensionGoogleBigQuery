using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Hosting;

[assembly: WebJobsStartup(typeof(AzureFunctions.Extensions.GoogleBigQuery.GoogleBigQueryWebJobsStartup))]
namespace AzureFunctions.Extensions.GoogleBigQuery.Config {

    public class GoogleBigQueryWebJobsStartup : IWebJobsStartup
    {
        public void Configure(IWebJobsBuilder builder)
        {
            builder.AddExtension(new GoogleBigQueryExtensionConfig());
        }
    }   

}
