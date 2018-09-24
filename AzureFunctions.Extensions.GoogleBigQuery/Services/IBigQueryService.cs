using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TransparentApiClient.Google.BigQuery.V2.Schema;
using TransparentApiClient.Google.Core;

namespace AzureFunctions.Extensions.GoogleBigQuery.Services {

    public interface ITableData {

        Task<BaseResponse<TableDataInsertAllResponse>> InsertRowsAsync(DateTime? date, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken);

    }

}