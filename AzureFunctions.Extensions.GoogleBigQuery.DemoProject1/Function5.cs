using System;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Azure.WebJobs;

namespace AzureFunctions.Extensions.GoogleBigQuery.DemoProject1 {
    public static class Function5 {

        [Disable]
        [FunctionName("Function5")]
        public static void Run(
            [TimerTrigger("0 */1 * * * *", RunOnStartup = true)]TimerInfo myTimer,
            [GoogleBigQuery("MyGoogleBigQueryConfig4")] GoogleBigQueryOperations bigQueryOperations) {


            bigQueryOperations.CreateTableAsync();
            bigQueryOperations.DeleteTableAsync();


        }
    }
}
