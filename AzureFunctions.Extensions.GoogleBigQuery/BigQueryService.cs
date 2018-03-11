using System.Collections.Generic;
using System.Linq;
using System;
//using Google.Cloud.BigQuery.V2;
//using Google.Apis.Auth.OAuth2;
//using Google.Apis.Bigquery.v2.Data;
using System.Threading.Tasks;
using System.Threading;
using TransparentApiClient.Google.BigQuery.V2.Resources;
using Newtonsoft.Json;
using TransparentApiClient.Google.Core;
using TransparentApiClient.Google.BigQuery.V2.Schema;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    public class BigQueryService {

        private const string BigQueryDateTimeFormat = "yyyy-MM-dd HH:mm:ss";

        private readonly GoogleBigQueryAttribute googleBigQueryAttribute;
        //private readonly TableSchema tableSchema;
        //private readonly IDictionary<string, IEnumerable<System.Reflection.PropertyInfo>> dictionaryOfProperties;

        public BigQueryService(GoogleBigQueryAttribute googleBigQueryAttribute/*, Type itemType*/) {
            this.googleBigQueryAttribute = GoogleBigQueryAttribute.GetAttributeByConfiguration(googleBigQueryAttribute);
            //if (itemType != null) {
            //    (this.tableSchema, this.dictionaryOfProperties) = TableSchemaBuilderService.GetTableSchema(itemType);
            //}
        }

        //public Task CreateTableAsync(bool timePartitioning, CancellationToken cancellationToken) {

        //    BigQueryClient client = GetBiqQueryClient();

        //    return client.GetOrCreateTableAsync(
        //                googleBigQueryAttribute.DatasetId,
        //                googleBigQueryAttribute.TableId,
        //                tableSchema,
        //                null,
        //                timePartitioning ? new CreateTableOptions() { TimePartitioning = new TimePartitioning() { Type = "DAY" } } : null,
        //                cancellationToken)
        //            .ContinueWith((getOrCreateTableTask) => {

        //                if (getOrCreateTableTask.IsFaulted) {

        //                    throw getOrCreateTableTask.Exception.GetBaseException();
        //                }

        //            });

        //}

        //public Task DeleteTableAsync(CancellationToken cancellationToken) {
        //    return DeleteTableAsync(googleBigQueryAttribute.DatasetId, googleBigQueryAttribute.TableId, cancellationToken);
        //}

        //public Task DeleteTableAsync(string tableId, CancellationToken cancellationToken) {
        //    return DeleteTableAsync(googleBigQueryAttribute.DatasetId, tableId, cancellationToken);
        //}

        //public Task DeleteTableAsync(string datasetId, string tableId, CancellationToken cancellationToken) {

        //    BigQueryClient client = GetBiqQueryClient();

        //    return client.DeleteTableAsync(
        //            datasetId,
        //            tableId,
        //            null,
        //            cancellationToken);

        //}

        //private Task<BigQueryTable> GetTableAsync(DateTime date, CancellationToken cancellationToken) {
        //    BigQueryClient client = GetBiqQueryClient();

        //    return client.GetTableAsync(googleBigQueryAttribute.DatasetId, $"{googleBigQueryAttribute.TableId}${date:yyyyMMdd}", null, cancellationToken);
        //}

        //private Task<BigQueryTable> GetTableAsync(CancellationToken cancellationToken) {
        //    BigQueryClient client = GetBiqQueryClient();

        //    return client.GetTableAsync(googleBigQueryAttribute.DatasetId, googleBigQueryAttribute.TableId, null, cancellationToken);
        //}

        private static Tabledata _client = null;

        private Tabledata GetBiqQueryClient() {

            if (_client != null) { return _client; }

            byte[] googleCredential = null;
            if (googleBigQueryAttribute.Credentials != null) {
                googleCredential = googleBigQueryAttribute.Credentials;
            } else {
                if (!string.IsNullOrWhiteSpace(googleBigQueryAttribute.CredentialsFileName)) {
                    var path = System.IO.Path.GetDirectoryName(typeof(GoogleBigQueryAttribute).Assembly.Location);
                    var fullPath = System.IO.Path.Combine(path, "..", googleBigQueryAttribute.CredentialsFileName);
                    var credentials = System.IO.File.ReadAllBytes(fullPath);
                    googleCredential = credentials;
                }
            }
            _client = new Tabledata(googleCredential);
            return _client;
        }

        public Task<BaseResponse<TableDataInsertAllResponse>> InsertRowsAsync(DateTime? date, IEnumerable<GoogleBigQueryRow> rows, CancellationToken cancellationToken) {

            if (rows != null && rows.Count() > 0) {

                //var bigQueryRows = rows.Select(c => BigQueryInsertRowService.GetBigQueryInsertRow(c, dictionaryOfProperties));

                //return GetTableAsync(date, cancellationToken)
                //    .ContinueWith((tableTask) => {
                //        BigQueryTable table = tableTask.Result;

                //        return table.InsertRowsAsync(bigQueryRows, new InsertOptions() { AllowUnknownFields = true }, cancellationToken)
                //                    .ContinueWith((insertRowsTask) => {
                //                        if (insertRowsTask.IsFaulted) {
                //                            throw insertRowsTask.Exception.InnerExceptions.First();
                //                        }
                //                    });
                //    }, cancellationToken).Unwrap();

                string tableName = googleBigQueryAttribute.TableId;
                if (date.HasValue) {
                    tableName = $"{googleBigQueryAttribute.TableId}${date.Value:yyyyMMdd}";
                }

                var settings = new JsonSerializerSettings() { DateFormatString = BigQueryDateTimeFormat };
                
                var insertAllTask = GetBiqQueryClient().InsertAllAsync(
                    googleBigQueryAttribute.DatasetId,
                    googleBigQueryAttribute.ProjectId,
                    tableName,
                    new TableDataInsertAllRequest() {
                        ignoreUnknownValues = true,
                        rows = rows.Select(c => new TableDataInsertAllRequest.Row() { insertId = c.__InsertId, json = c })
                    }, cancellationToken);

                return insertAllTask;
            }

            return Task.FromResult<BaseResponse<TableDataInsertAllResponse>>(null);
        }

    }
}