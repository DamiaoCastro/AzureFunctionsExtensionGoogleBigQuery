using System;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Azure.WebJobs;

namespace AzureFunctions.Extensions.GoogleBigQuery.DemoProject1 {

    public static class Function1 {

        public class MyBigQueryRow : GoogleBigQueryRow {
            public MyBigQueryRow(DateTime date, string insertId) : base(date, insertId) {
            }

            [Column]
            public int SomeIntegerValue { get; set; }

        }

        [FunctionName("Function1")]
        public static void Run(
            [TimerTrigger("0 */5 * * * *")]TimerInfo myTimer,
            [GoogleBigQuery("credencials.json", "projectId", "datasetId","tableId")]
                ICollector<MyBigQueryRow> rows) {
            
            rows.Add(new MyBigQueryRow(DateTime.UtcNow, "insertId") { SomeIntegerValue = 1 });

        }

    }
}
