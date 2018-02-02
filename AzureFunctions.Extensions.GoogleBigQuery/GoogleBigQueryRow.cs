using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {
    public class GoogleBigQueryRow {

        public GoogleBigQueryRow(DateTime? date, string insertId) {
            Date = date;
            InsertId = insertId;
        }

        public DateTime? Date { get; }
        public string InsertId { get; }

    }
}