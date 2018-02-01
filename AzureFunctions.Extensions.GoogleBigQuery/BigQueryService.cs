using System.Collections.Generic;
using System.Linq;
using System;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using System.Threading.Tasks;
using System.Threading;

namespace AzureFunctions.Extensions.GoogleBigQuery
{

    public class BigQueryService
    {

        private readonly GoogleBigQueryAttribute googleBigQueryAttribute;
        private readonly TableSchema tableSchema;
        private readonly IDictionary<string, IEnumerable<System.Reflection.PropertyInfo>> dictionaryOfProperties;

        public BigQueryService(GoogleBigQueryAttribute googleBigQueryAttribute, Type itemType)
        {
            this.googleBigQueryAttribute = GoogleBigQueryAttribute.GetAttributeByConfiguration(googleBigQueryAttribute);
            if (itemType != null)
            {
                (this.tableSchema, this.dictionaryOfProperties) = TableSchemaBuilderService.GetTableSchema(itemType);
            }
        }

        public Task CreateTableAsync(bool timePartitioning, CancellationToken cancellationToken)
        {

            BigQueryClient client = GetBiqQueryClient();

            return client.GetOrCreateTableAsync(
                        googleBigQueryAttribute.DatasetId,
                        googleBigQueryAttribute.TableId,
                        tableSchema,
                        null,
                        timePartitioning ? new CreateTableOptions() { TimePartitioning = new TimePartitioning() { Type = "DAY" } } : null,
                        cancellationToken)
                    .ContinueWith((getOrCreateTableTask) => {

                        if (getOrCreateTableTask.IsFaulted) {

                            throw getOrCreateTableTask.Exception.GetBaseException();
                        }

                    });

        }

        public Task DeleteTableAsync(CancellationToken cancellationToken)
        {
            return DeleteTableAsync(googleBigQueryAttribute.DatasetId, googleBigQueryAttribute.TableId, cancellationToken);
        }

        public Task DeleteTableAsync(string tableId, CancellationToken cancellationToken)
        {
            return DeleteTableAsync(googleBigQueryAttribute.DatasetId, tableId, cancellationToken);
        }

        public Task DeleteTableAsync(string datasetId, string tableId, CancellationToken cancellationToken)
        {

            BigQueryClient client = GetBiqQueryClient();

            return client.DeleteTableAsync(
                    datasetId,
                    tableId,
                    null,
                    cancellationToken);

        }

        private Task<BigQueryTable> GetTableAsync(DateTime date, CancellationToken cancellationToken)
        {
            BigQueryClient client = GetBiqQueryClient();

            return client.GetTableAsync(googleBigQueryAttribute.DatasetId, $"{googleBigQueryAttribute.TableId}${date:yyyyMMdd}", null, cancellationToken);
        }

        private Task<BigQueryTable> GetTableAsync(CancellationToken cancellationToken)
        {
            BigQueryClient client = GetBiqQueryClient();

            return client.GetTableAsync(googleBigQueryAttribute.DatasetId, googleBigQueryAttribute.TableId, null, cancellationToken);
        }

        private static BigQueryClient _client = null;

        private BigQueryClient GetBiqQueryClient()
        {

            if (_client != null) { return _client; }

            GoogleCredential googleCredential = null;
            if (googleBigQueryAttribute.Credentials != null)
            {
                googleCredential = GoogleCredential.FromStream(new System.IO.MemoryStream(googleBigQueryAttribute.Credentials));
            }
            else
            {
                if (!string.IsNullOrWhiteSpace(googleBigQueryAttribute.CredentialsFileName))
                {
                    var path = System.IO.Path.GetDirectoryName(typeof(GoogleBigQueryAttribute).Assembly.Location);
                    var fullPath = System.IO.Path.Combine(path, "..", googleBigQueryAttribute.CredentialsFileName);
                    var credentials = System.IO.File.ReadAllBytes(fullPath);
                    googleCredential = GoogleCredential.FromStream(new System.IO.MemoryStream(credentials));
                }
            }
            _client = BigQueryClient.Create(googleBigQueryAttribute.ProjectId, googleCredential);
            return _client;
        }

        public Task InsertRowsAsync(DateTime date, IEnumerable<GoogleBigQueryRow> rows, CancellationToken cancellationToken)
        {

            if (rows != null && rows.Count() > 0)
            {
                int dateDiff = (date - DateTime.UtcNow.Date).Days;

                if (dateDiff >= -31 && dateDiff <= 16)
                {

                    var bigQueryRows = rows.Select(c => BigQueryInsertRowService.GetBigQueryInsertRow(c, dictionaryOfProperties));

                    return GetTableAsync(date, cancellationToken)
                        .ContinueWith((tableTask) =>
                        {
                            BigQueryTable table = tableTask.Result;

                            return table.InsertRowsAsync(bigQueryRows, new InsertOptions() { AllowUnknownFields = true }, cancellationToken)
                                        .ContinueWith((insertRowsTask) =>
                                        {
                                            if (insertRowsTask.IsFaulted)
                                            {
                                                throw insertRowsTask.Exception.InnerExceptions.First();
                                            }
                                        });
                        }, cancellationToken).Unwrap();

                }
                else
                {
                    throw new ArgumentOutOfRangeException("BigQuery streamming API don't allow to write data in DAY partioned tabled outside 31 days in the past and 16 days in the future.");
                }
            }

            return Task.CompletedTask;
        }

        public async Task InsertRowsAsync(IEnumerable<GoogleBigQueryRow> rows, CancellationToken cancellationToken)
        {

            if (rows != null && rows.Count() > 0)
            {

                await GetTableAsync(cancellationToken)
                    .ContinueWith((tableTask) =>
                    {
                        BigQueryTable table = tableTask.Result;
                        
                        var bigQueryRows = rows.Select(c => BigQueryInsertRowService.GetBigQueryInsertRow(c, dictionaryOfProperties)).ToArray();
                        
                        return table.InsertRowsAsync(bigQueryRows, new InsertOptions() { AllowUnknownFields = false }, cancellationToken)
                                    .ContinueWith((insertRowsTask) =>
                                    {
                                        if (insertRowsTask.IsFaulted)
                                        {
                                            throw insertRowsTask.Exception.InnerExceptions.First();
                                        }
                                    });
                    }, cancellationToken).Unwrap();

            }

            //return Task.CompletedTask;
        }
    }
}