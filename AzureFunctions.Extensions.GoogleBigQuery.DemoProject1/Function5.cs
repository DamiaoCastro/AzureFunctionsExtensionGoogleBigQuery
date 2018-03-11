using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;

namespace AzureFunctions.Extensions.GoogleBigQuery.DemoProject1 {
    public static class Function5 {

        public class MyBigQueryRow : GoogleBigQueryRow {
            public MyBigQueryRow(DateTime date, string insertId) : base(date, insertId) {
            }

            [Column]
            public string FunctionName1 { get; set; }

            [Column]
            public Int64 SomeIntegerValue1 { get; set; }

        }

        [FunctionName("Function5")]
        public static void Run(
            [TimerTrigger("0 */1 * * * *", RunOnStartup = true)]TimerInfo myTimer,
            [GoogleBigQueryManagement("MyGoogleBigQueryConfig4")] GoogleBigQueryManagement management,
            CancellationToken cancellationToken) {

            //return management.CreateTableAsync<MyBigQueryRow>(true, cancellationToken);
            //return management.DeleteTableAsync("table2$20180113", cancellationToken);

        }
    }
}
