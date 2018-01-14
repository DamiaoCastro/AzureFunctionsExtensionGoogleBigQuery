using System.Collections.Generic;
using System.Linq;
using System;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using System.Threading.Tasks;
using System.Threading;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    public class BigQueryService {

        private readonly GoogleBigQueryAttribute googleBigQueryAttribute;
        private readonly TableSchema tableSchema;
        private readonly IDictionary<string, IEnumerable<System.Reflection.PropertyInfo>> dictionaryOfProperties;

        public BigQueryService(GoogleBigQueryAttribute googleBigQueryAttribute, Type itemType) {
            this.googleBigQueryAttribute = GoogleBigQueryAttribute.GetAttributeByConfiguration(googleBigQueryAttribute);
            (this.tableSchema, this.dictionaryOfProperties) = TableSchemaBuilderService.GetTableSchema(itemType);
        }

        private Task<BigQueryTable> GetTable(DateTime date, CancellationToken cancellationToken) {

            GoogleCredential googleCredential = null;
            if (googleBigQueryAttribute.Credentials != null) {
                googleCredential = GoogleCredential.FromStream(new System.IO.MemoryStream(googleBigQueryAttribute.Credentials));
            } else {
                if (!string.IsNullOrWhiteSpace(googleBigQueryAttribute.CredentialsFileName)) {
                    var path = System.IO.Path.GetDirectoryName(typeof(GoogleBigQueryAttribute).Assembly.Location);
                    var fullPath = System.IO.Path.Combine(path, "..", googleBigQueryAttribute.CredentialsFileName);
                    var credentials = System.IO.File.ReadAllBytes(fullPath);
                    googleCredential = GoogleCredential.FromStream(new System.IO.MemoryStream(credentials));
                }
            }
            var client = BigQueryClient.Create(googleBigQueryAttribute.ProjectId, googleCredential);

            //return client.GetOrCreateTableAsync(datasetId, tableId, tableSchema, null, new CreateTableOptions() { TimePartitioning = new TimePartitioning() { Type = "DAY" } }, cancellationToken)
            //            .ContinueWith((createTableTask) => {
            //                return client.GetTableAsync(datasetId, $"{tableId}${date:yyyyMMdd}", null, cancellationToken);
            //            }, cancellationToken).Unwrap();
            return client.GetTableAsync(googleBigQueryAttribute.DatasetId, $"{googleBigQueryAttribute.TableId}${date:yyyyMMdd}", null, cancellationToken);
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
                                                throw insertRowsTask.Exception.InnerExceptions.First();
                                            }
                                        });
                        }, cancellationToken).Unwrap();

                }
            }

            return Task.WhenAll();
        }

    }
}