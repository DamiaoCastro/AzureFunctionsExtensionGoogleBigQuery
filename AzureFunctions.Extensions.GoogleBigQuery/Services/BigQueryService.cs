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

        private readonly GoogleBigQueryCollectorAttribute googleBigQueryCollectorAttribute;
        private readonly ITabledata tabledata;

        public BigQueryService(GoogleBigQueryCollectorAttribute googleBigQueryCollectorAttribute, ITabledata tabledata) {
            this.googleBigQueryCollectorAttribute = googleBigQueryCollectorAttribute ?? throw new ArgumentNullException(nameof(googleBigQueryCollectorAttribute));
            this.tabledata = tabledata ?? throw new ArgumentNullException(nameof(tabledata));
        }

        Task<BaseResponse<TableDataInsertAllResponse>> IBigQueryService.InsertRowsAsync(DateTime? date, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) {

            if (rows != null && rows.Count() > 0) {

                string tableName = googleBigQueryCollectorAttribute.TableId;
                if (date.HasValue) {
                    tableName = $"{googleBigQueryCollectorAttribute.TableId}${date.Value:yyyyMMdd}";
                }

                var settings = new JsonSerializerSettings() { DateFormatString = BigQueryDateTimeFormat };

                return tabledata.InsertAllAsync(
                    googleBigQueryCollectorAttribute.DatasetId,
                    googleBigQueryCollectorAttribute.ProjectId,
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