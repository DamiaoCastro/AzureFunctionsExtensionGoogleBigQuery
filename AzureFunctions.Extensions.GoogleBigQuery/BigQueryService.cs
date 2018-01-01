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

        private readonly byte[] credentials;
        private readonly string projectId;
        private readonly string datasetId;
        private readonly string tableId;
        private readonly TableSchema tableSchema;
        private readonly IDictionary<string, IEnumerable<System.Reflection.PropertyInfo>> dictionaryOfProperties;

        public BigQueryService(byte[] credentials, string projectId, string datasetId, string tableId, Type itemType) {
            this.credentials = credentials;
            this.projectId = projectId;
            this.datasetId = datasetId;
            this.tableId = tableId;
            (this.tableSchema, this.dictionaryOfProperties) = TableSchemaBuilderService.GetTableSchema(itemType);
        }

        private Task<BigQueryTable> GetTable(DateTime date, CancellationToken cancellationToken) {

            GoogleCredential googleCredential = null;
            if (credentials != null) {
                googleCredential = GoogleCredential.FromStream(new System.IO.MemoryStream(credentials));
            }
            var client = Google.Cloud.BigQuery.V2.BigQueryClient.Create(projectId, googleCredential);
            
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

                    var bigQueryRows = rows.Select(c => BigQueryInsertRowService.GetBigQueryInsertRow(c, dictionaryOfProperties));
                    
                    return GetTable(date, cancellationToken)
                        .ContinueWith((tableTask) => {
                            BigQueryTable table = tableTask.Result;
                            
                            return table.InsertRowsAsync(bigQueryRows, new InsertOptions() { AllowUnknownFields = true }, cancellationToken)
                                        .ContinueWith((insertRowsTask) => {
                                            if (insertRowsTask.IsFaulted) {
                                                //... log trace
                                            }
                                        });
                        }, cancellationToken).Unwrap();

                }
            }

            return Task.WhenAll();
        }
        
    }
}