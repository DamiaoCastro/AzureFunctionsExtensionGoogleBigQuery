using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {
    public class GoogleBigQueryRow {

        public GoogleBigQueryRow(DateTime? date, string insertId) {
            __Date = date;
            __InsertId = insertId;
        }

        [Newtonsoft.Json.JsonIgnore]
        public DateTime? __Date { get; }
        [Newtonsoft.Json.JsonIgnore]
        public string __InsertId { get; }

    }
}