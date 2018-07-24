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

namespace AzureFunctions.Extensions.GoogleBigQuery
{

    public class BigQueryService
    {

        private const string BigQueryDateTimeFormat = "yyyy-MM-dd HH:mm:ss";

        private readonly GoogleBigQueryAttribute googleBigQueryAttribute;
        //private readonly TableSchema tableSchema;
        //private readonly IDictionary<string, IEnumerable<System.Reflection.PropertyInfo>> dictionaryOfProperties;

        public BigQueryService(GoogleBigQueryAttribute googleBigQueryAttribute/*, Type itemType*/)
        {
            this.googleBigQueryAttribute = GoogleBigQueryAttribute.GetAttributeByConfiguration(googleBigQueryAttribute);
            //if (itemType != null) {
            //    (this.tableSchema, this.dictionaryOfProperties) = TableSchemaBuilderService.GetTableSchema(itemType);
            //}
        }

        internal Task<BaseResponse<QueryResponse>> QueryAsync(string query, Dictionary<string, string> parameters, CancellationToken cancellationToken)
        {
            var jobsClient = GetBiqQueryJobsClient();

            return jobsClient.QueryAsync(
                googleBigQueryAttribute.ProjectId,
                new QueryRequest()
                {
                    query = query,
                    useLegacySql = false,
                    useQueryCache = false,
                    parameterMode = "NAMED",
                    queryParameters = parameters.Select(c => new QueryParameter()
                    {
                        name = c.Key,
                        parameterValue = new QueryParameterValue()
                        {
                            value = c.Value
                        }
                    })
                },
                null, cancellationToken);
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

        private static Tabledata _TabledataClient = null;

        private Tabledata GetBiqQueryTabledataClient()
        {
            if (_TabledataClient != null) { return _TabledataClient; }
            byte[] googleCredential = GetCredentials();
            _TabledataClient = new Tabledata(googleCredential);
            return _TabledataClient;
        }

        private static Jobs _JobsClient = null;

        private Jobs GetBiqQueryJobsClient()
        {
            if (_JobsClient != null) { return _JobsClient; }
            byte[] googleCredential = GetCredentials();
            _JobsClient = new Jobs(googleCredential);
            return _JobsClient;
        }

        private byte[] GetCredentials()
        {
            byte[] googleCredential = null;
            if (googleBigQueryAttribute.Credentials != null)
            {
                googleCredential = googleBigQueryAttribute.Credentials;
            }
            else
            {
                if (!string.IsNullOrWhiteSpace(googleBigQueryAttribute.CredentialsFileName))
                {
                    var path = System.IO.Path.GetDirectoryName(typeof(GoogleBigQueryAttribute).Assembly.Location);
                    var fullPath = System.IO.Path.Combine(path, "..", googleBigQueryAttribute.CredentialsFileName);
                    var credentials = System.IO.File.ReadAllBytes(fullPath);
                    googleCredential = credentials;
                }
            }

            return googleCredential;
        }

        public Task<BaseResponse<TableDataInsertAllResponse>> InsertRowsAsync(DateTime? date, IEnumerable<GoogleBigQueryRow> rows, CancellationToken cancellationToken)
        {

            if (rows != null && rows.Count() > 0)
            {

                string tableName = googleBigQueryAttribute.TableId;
                if (date.HasValue)
                {
                    tableName = $"{googleBigQueryAttribute.TableId}${date.Value:yyyyMMdd}";
                }

                var settings = new JsonSerializerSettings() { DateFormatString = BigQueryDateTimeFormat };

                var insertAllTask = GetBiqQueryTabledataClient().InsertAllAsync(
                    googleBigQueryAttribute.DatasetId,
                    googleBigQueryAttribute.ProjectId,
                    tableName,
                    new TableDataInsertAllRequest()
                    {
                        ignoreUnknownValues = true,
                        rows = rows.Select(c => new TableDataInsertAllRequest.Row() { insertId = c.__InsertId, json = c })
                    },
                    settings,
                    cancellationToken);

                return insertAllTask;
            }

            return Task.FromResult<BaseResponse<TableDataInsertAllResponse>>(null);
        }

    }
}