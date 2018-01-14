using System;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Azure.WebJobs;

namespace AzureFunctions.Extensions.GoogleBigQuery.DemoProject1 {
    public static class Function3 {

        public class MyBigQueryRow : GoogleBigQueryRow {
            public MyBigQueryRow(DateTime date, string insertId) : base(date, insertId) {
            }

            [Column]
            public string FunctionName { get; set; }

            [Column]
            public Int64 SomeIntegerValue { get; set; }

        }

        [Disable]
        [FunctionName("Function3")]
        public static void Run([TimerTrigger("0 */5 * * * *", RunOnStartup = true)]TimerInfo myTimer,
            [GoogleBigQuery("MyGoogleBigQueryConfig3")]
                ICollector<MyBigQueryRow> rows) {

            rows.Add(new MyBigQueryRow(DateTime.UtcNow, "insertId3") { FunctionName = "Function3", SomeIntegerValue = Int64.Parse(DateTime.UtcNow.ToString("yyyyMMddHHmm")) });

        }
    }
}
