using Newtonsoft.Json.Linq;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery
{
    public class GoogleBigQueryRow : JObject
    {

        public GoogleBigQueryRow(DateTime? date, string insertId)
        {
            __Date = date;
            __InsertId = insertId;
        }
        public GoogleBigQueryRow(DateTime? date, string insertId, JObject jObject)
        {
            __Date = date;
            __InsertId = insertId;
            Add(jObject.Children());
        }

        [Newtonsoft.Json.JsonIgnore]
        public DateTime? __Date { get; set; }
        [Newtonsoft.Json.JsonIgnore]
        public string __InsertId { get; set; }

    }
}