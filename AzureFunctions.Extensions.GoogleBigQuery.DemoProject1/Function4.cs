using System;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Azure.WebJobs;

namespace AzureFunctions.Extensions.GoogleBigQuery.DemoProject1 {
    public static class Function4 {

        public class MyBigQueryRow : GoogleBigQueryRow {
            public MyBigQueryRow(DateTime date, string insertId) : base(date, insertId) {
            }

            [Column]
            public string FunctionName { get; set; }

            [Column]
            public Int64 SomeIntegerValue { get; set; }

        }

        [Disable]
        [FunctionName("Function4")]
        public static void Run([TimerTrigger("0 */1 * * * *", RunOnStartup = true)]TimerInfo myTimer,
            [GoogleBigQuery("MyGoogleBigQueryConfig4")]
                ICollector<MyBigQueryRow> rows) {

            rows.Add(new MyBigQueryRow(DateTime.UtcNow, "insertId4") { FunctionName = "Function4", SomeIntegerValue = Int64.Parse(DateTime.UtcNow.ToString("yyyyMMddHHmm")) });

        }
    }
}
