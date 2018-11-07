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

    public class BigQueryService : ITableData {

        private const string BigQueryDateTimeFormat = "yyyy-MM-dd HH:mm:ss";

        private readonly GoogleBigQueryAttribute googleBigQueryAttribute;
        private readonly ITableDataClientCacheService tableDataClientCacheService;

        public BigQueryService(GoogleBigQueryAttribute googleBigQueryAttribute, ITableDataClientCacheService tableDataClientCacheService) {
            this.googleBigQueryAttribute = googleBigQueryAttribute;
            this.tableDataClientCacheService = tableDataClientCacheService;
        }

        Task<BaseResponse<TableDataInsertAllResponse>> ITableData.InsertRowsAsync(DateTime? date, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) {

            if (rows != null && rows.Count() > 0) {

                string tableName = googleBigQueryAttribute.TableId;
                if (date.HasValue) {
                    tableName = $"{googleBigQueryAttribute.TableId}${date.Value:yyyyMMdd}";
                }

                var settings = new JsonSerializerSettings() { DateFormatString = BigQueryDateTimeFormat };

                ITabledata tabledataClient = tableDataClientCacheService.GetTabledataClient(googleBigQueryAttribute);

                return tabledataClient.InsertAllAsync(
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