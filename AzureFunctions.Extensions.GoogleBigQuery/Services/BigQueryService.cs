using System.Collections.Generic;
using System.Linq;
using System;
using System.Threading.Tasks;
using System.Threading;
using TransparentApiClient.Google.BigQuery.V2.Resources;
using Newtonsoft.Json;
using TransparentApiClient.Google.Core;
using TransparentApiClient.Google.BigQuery.V2.Schema;

namespace AzureFunctions.Extensions.GoogleBigQuery.Services {

    public class BigQueryService : IBigQueryService {

        private const string BigQueryDateTimeFormat = "yyyy-MM-dd HH:mm:ss";

        private readonly GoogleBigQueryAttribute googleBigQueryAttribute;

        public BigQueryService(GoogleBigQueryAttribute googleBigQueryAttribute) {
            this.googleBigQueryAttribute = GoogleBigQueryAttribute.GetAttributeByConfiguration(googleBigQueryAttribute);
        }

        private static Tabledata _client = null;

        private ITabledata GetBiqQueryClient() {

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

        Task<BaseResponse<TableDataInsertAllResponse>> IBigQueryService.InsertRowsAsync(DateTime? date, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) {

            if (rows != null && rows.Count() > 0) {

                string tableName = googleBigQueryAttribute.TableId;
                if (date.HasValue) {
                    tableName = $"{googleBigQueryAttribute.TableId}${date.Value:yyyyMMdd}";
                }

                var settings = new JsonSerializerSettings() { DateFormatString = BigQueryDateTimeFormat };

                return GetBiqQueryClient().InsertAllAsync(
                    googleBigQueryAttribute.DatasetId,
                    googleBigQueryAttribute.ProjectId,
                    tableName,
                    new TableDataInsertAllRequest() {
                        ignoreUnknownValues = true,
                        rows = rows.Select(c => new TableDataInsertAllRequest.Row() { insertId = c.getInsertId(), json = c })
                    },
                    settings,
                    cancellationToken);

            }

            return Task.FromResult<BaseResponse<TableDataInsertAllResponse>>(null);
        }

    }
}