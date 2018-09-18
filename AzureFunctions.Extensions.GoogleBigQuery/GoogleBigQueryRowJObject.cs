using Newtonsoft.Json.Linq;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery
{
    public class GoogleBigQueryRowJObject : JObject, IGoogleBigQueryRow
    {
        private DateTime? __Date;
        private string __InsertId;

        public GoogleBigQueryRowJObject(DateTime? date, string insertId, JObject jObject)
        {
            __Date = date;
            __InsertId = insertId;
            Add(jObject.Children());
        }

        DateTime? IGoogleBigQueryRow.getPartitionDate()
        {
            return __Date;
        }

        string IGoogleBigQueryRow.getInsertId()
        {
            return __InsertId;
        }

    }
}
