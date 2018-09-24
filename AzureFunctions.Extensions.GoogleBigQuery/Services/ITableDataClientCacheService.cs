using System;
using TransparentApiClient.Google.BigQuery.V2.Resources;

namespace AzureFunctions.Extensions.GoogleBigQuery.Services {
    public interface ITableDataClientCacheService {

        ITabledata GetTabledataClient(GoogleBigQueryAttribute googleBigQueryAttribute);

    }
}
