using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;

namespace AzureFunctions.Extensions.GoogleBigQuery {
    public class BqRow {

        private IDictionary<string, object> fields = new Dictionary<string, object>();

        public BqRow() { }

        public BqRow(string insertId) { InsertId = insertId; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="date">For DATE partitioned tables! This information will be the one to determine what partition the row will be written to. Do not fill this information if the table is not partitioned or is automatically sent to a partition based on a column.</param>
        /// <param name="insertId"></param>
        public BqRow(DateTime? date, string insertId) { Date = date; InsertId = insertId; }

        //
        // Summary:
        //     Accesses a field in the row by name.
        //
        // Parameters:
        //   name:
        //     The name of the field to access. Must be a valid BigQuery field name.
        //
        // Returns:
        //     The value associated with name.
        //
        // Exceptions:
        //   T:System.Collections.Generic.KeyNotFoundException:
        //     The row does not have a key with the given name.
        public object this[string name] { get { return fields[name]; } set { fields.Add(name, value); } }

        /// <summary>
        /// For DATE partitioned tables! This information will be the one to determine what partition the row will be written to.
        /// Do not fill this information if the table is not partitioned or is automatically sent to a partition based on a column.
        /// </summary>
        public DateTime? Date { get; }

        ///<summary>
        /// Summary:
        ///     To help ensure data consistency, you can supply an Google.Cloud.BigQuery.V2.BigQueryInsertRow.InsertId
        ///     for each inserted row. BigQuery remembers this ID for at least one minute. If
        ///     you try to stream the same set of rows within that time period and the insertId
        ///     property is set, BigQuery uses the property to de-duplicate your data on a best
        ///     effort basis. By default if no ID is specified, one will be generated to allow
        ///     de-duplicating efforts if insert operations need to be retried. You can allow
        ///     empty Google.Cloud.BigQuery.V2.BigQueryInsertRow.InsertId by setting Google.Cloud.BigQuery.V2.InsertOptions.AllowEmptyInsertIds
        ///     to true. This will allow for faster row inserts at the expense of possible record
        ///     duplication if the operation needs to be retried. See https://cloud.google.com/bigquery/quotas#streaming_inserts
        ///     for more information. 
        ///</summary>       
        public string InsertId { get; }

        /// <summary>
        /// Adds all the values in the specified dictionary to the row.
        /// </summary>
        /// <param name="fields">The fields to add to the row. Must not be null.</param>
        /// <remarks>
        ///     This being named Add rather than AddRange allows it to be specified in a collection
        ///     initializer, which can be useful to provide a set of common fields and then some
        ///     extra values.
        /// </remarks>
        public void Add(IDictionary<string, object> fields) {
            foreach (var field in fields) {
                this.fields.Add(field);
            }
        }

        /// Summary:
        ///     Adds a single field value to the row.
        ///
        /// Parameters:
        ///   key:
        ///     The name of the field. Must be a valid BigQuery field name.
        ///
        ///   value:
        ///     The value for the field, which must be null or one of the supported types.
        public void Add(string key, object value) {
            this.fields.Add(key, value);
        }

        internal BigQueryInsertRow AsBigQueryInsertRow() {
            var bigQueryInsertRow = new BigQueryInsertRow(InsertId);
            bigQueryInsertRow.Add(fields);
            return bigQueryInsertRow;
        }

    }
}
