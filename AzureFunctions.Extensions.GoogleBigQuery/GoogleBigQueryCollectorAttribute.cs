using Microsoft.Azure.WebJobs.Description;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    [Binding]
    [AttributeUsage(AttributeTargets.Parameter)]
    public sealed class GoogleBigQueryCollectorAttribute : Attribute {

        /// <summary>
        /// Use this attribute to bind to ICollector<IGoogleBigQueryRow>, meaning ICollector<*My*GoogleBigQueryRow> or ICollector<GoogleBigQueryRowJObject>.
        /// The values collected will be writen using the streaming API of BigQuery. Please note that the streaming API, when writting to DAY partioned tables has a data range limitation.
        /// More information about BigQuery streaming api in the page. https://cloud.google.com/bigquery/streaming-data-into-bigquery
        /// </summary>
        /// <param name="fullTableId">format `datasetId.tableId`</param>
        public GoogleBigQueryCollectorAttribute(string fullTableId) {
            if (string.IsNullOrWhiteSpace(fullTableId)) { throw new ArgumentException($"'{nameof(fullTableId)}' cannot be null or whitespace", nameof(fullTableId)); }

            FullTableId = fullTableId;
            var split = fullTableId.Split('.');
            DatasetId = split[0];
            TableId = split[1];
        }

        /// <summary>
        /// format `datasetId.tableId`
        /// </summary>
        public string FullTableId { get; }

        public string DatasetId { get; }

        public string TableId { get; }

    }
}