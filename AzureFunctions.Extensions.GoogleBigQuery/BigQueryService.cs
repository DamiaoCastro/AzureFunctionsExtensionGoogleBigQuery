using System.Collections.Generic;
using System.Linq;
using System;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using System.Threading.Tasks;
using System.Reflection;
using System.Threading;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    public class BigQueryService {

        private const string BigQueryDateTimeFormat = "yyyy-MM-dd HH:mm:ss";
        private System.Globalization.CultureInfo cultureUS = new System.Globalization.CultureInfo("en-US");

        private readonly byte[] credentials;
        private readonly string projectId;
        private readonly string datasetId;
        private readonly string tableId;
        private readonly TableSchema tableSchema;
        private readonly IEnumerable<PropertyInfo> properties;

        public BigQueryService(byte[] credentials, string projectId, string datasetId, string tableId, Type itemType) {
            this.credentials = credentials;
            this.projectId = projectId;
            this.datasetId = datasetId;
            this.tableId = tableId;
            (this.tableSchema, this.properties) = TableSchemaBuilderService.GetTableSchema(itemType);
        }

        //private (TableSchema, IEnumerable<System.Reflection.PropertyInfo>) GetTableSchema(Type tableType) {

        //    var properties = tableType.GetProperties()
        //            .Where(c => c.PropertyType.IsPublic && c.CustomAttributes.Any(a => a.AttributeType == typeof(ColumnAttribute)));

        //    var fields = from property in properties
        //                 let type = GetBigQueryType(property.PropertyType)
        //                 let mode = property.CustomAttributes.Any(a => a.AttributeType == typeof(RequiredAttribute)) ? "REQUIRED" : "NULLABLE"
        //                 select new TableFieldSchema() { Name = property.Name, Type = type, Mode = mode }
        //                 ;

        //    var schema = new Google.Apis.Bigquery.v2.Data.TableSchema() { Fields = fields.ToList() };

        //    return (schema, properties);
        //}

        //private string GetBigQueryType(Type propertyType) {

        //    //STRING
        //    //BYTES
        //    //INTEGER   
        //    //FLOAT
        //    //BOOLEAN
        //    //TIMESTAMP
        //    //DATE
        //    //TIME
        //    //DATETIME
        //    //RECORD

        //    if (propertyType.Name.Equals("Nullable`1")) {
        //        propertyType = propertyType.GenericTypeArguments[0];
        //    }

        //    switch (propertyType.Name.ToUpper()) {
        //        case "INT":
        //        case "INT32":
        //        case "INT64":
        //            return "INTEGER";
        //        default:
        //            return propertyType.Name.ToUpper();
        //    }
        //}

        private Task<BigQueryTable> GetTable(DateTime date, CancellationToken cancellationToken) {

            GoogleCredential googleCredential = null;
            if (credentials != null) {
                googleCredential = GoogleCredential.FromStream(new System.IO.MemoryStream(credentials));
            }
            var client = Google.Cloud.BigQuery.V2.BigQueryClient.Create(projectId, googleCredential);


            //client.GetTable

            //tableSchema.Fields.Insert(0, new TableFieldSchema() { Name = "_PARTITIONTIME", Mode = "REQUIRED", Type = "DATE" });

            return client.GetOrCreateTableAsync(
                        datasetId,
                        $"{tableId}${date.ToString("yyyyMMdd")}",
                        //tableId,
                        tableSchema,
                        new GetTableOptions(),
                        new CreateTableOptions() {
                            FriendlyName = $"{tableId}${date.ToString("yyyyMMdd")}",
                            TimePartitioning = new TimePartitioning() { Type = "DAY"/*, Field = "_PARTITIONTIME"*/ }
                        },
                        cancellationToken);
        }

        public Task InsertRowsAsync(DateTime date, IEnumerable<GoogleBigQueryRow> rows, CancellationToken cancellationToken) {

            if (rows != null && rows.Count() > 0) {
                int dateDiff = (date - DateTime.UtcNow.Date).Days;

                if (dateDiff >= -31 && dateDiff <= 16) {

                    var bigQueryRows = from r in rows
                                       let dic = properties.ToDictionary(c => c.Name, c => GetBigQueryValue(c, r))
                                       select new BigQueryInsertRow(r.InsertId) { dic };

                    return GetTable(date, cancellationToken)
                        .ContinueWith((tableTask) => {
                            BigQueryTable table = tableTask.Result;
                            return table.InsertRowsAsync(bigQueryRows, new InsertOptions() { AllowUnknownFields = true }, cancellationToken);
                        }, cancellationToken).Unwrap();

                }
            }

            return Task.WhenAll();
        }

        private object GetBigQueryValue(PropertyInfo property, GoogleBigQueryRow row) {
            switch (property.PropertyType.Name.ToUpper()) {
                case "BYTE":
                    return (int)(byte)property.GetValue(row);
                case "CHAR":
                    return ((char)property.GetValue(row)).ToString();
                case "CHAR[]":
                    return ((char[])property.GetValue(row)).ToString();
                case "DATETIME":
                    var datetimeValue = (DateTime)property.GetValue(row);
                    return datetimeValue.ToString(BigQueryDateTimeFormat, cultureUS);
                case "DECIMAL":
                    return (float)(decimal)property.GetValue(row);
                case "GUID":
                    return ((Guid)property.GetValue(row)).ToString();
                case "UINT64":
                    return (int)(UInt64)property.GetValue(row);
                case "IENUMERABLE`1":
                    var enumerableValue = property.GetValue(row);
                    Type innerPropertyType = property.PropertyType.GenericTypeArguments[0];
                    switch (innerPropertyType.Name.ToUpper()) {
                        case "BOOLEAN":
                            return new bool[] { };
                        case "CHAR":
                            return ((char[])enumerableValue).ToString();
                        case "DECIMAL":
                            return ((decimal[])enumerableValue).Select(c => (float)c);
                        case "SINGLE":
                            //return ((Single[])enumerableValue).Select(c => (float)c);
                            return new float[] { };
                    }

                    return enumerableValue;
                default:
                    return property.GetValue(row);
            }
        }

    }
}
