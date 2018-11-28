using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AzureFunctions.Extensions.GoogleBigQuery.Services;
using Microsoft.Azure.WebJobs;
using TransparentApiClient.Google.BigQuery.V2.Schema;
using TransparentApiClient.Google.Core;

namespace AzureFunctions.Extensions.GoogleBigQuery.Bindings {

    public class GoogleBigQueryAsyncCollector : ICollector<IGoogleBigQueryRow>, IAsyncCollector<IGoogleBigQueryRow>, IDisposable {

        private readonly IBigQueryService bigQueryService;
        private List<IGoogleBigQueryRow> items = new List<IGoogleBigQueryRow>();

        public GoogleBigQueryAsyncCollector(IBigQueryService bigQueryService) {
            this.bigQueryService = bigQueryService;
        }

        void ICollector<IGoogleBigQueryRow>.Add(IGoogleBigQueryRow item) {
            if (item == null) { throw new ArgumentNullException(nameof(item)); }
            items.Add(item);
        }

        Task IAsyncCollector<IGoogleBigQueryRow>.AddAsync(IGoogleBigQueryRow item, CancellationToken cancellationToken) {
            if (item == null) { throw new ArgumentNullException(nameof(item)); }
            items.Add(item);
            return Task.CompletedTask;
        }

        Task IAsyncCollector<IGoogleBigQueryRow>.FlushAsync(CancellationToken cancellationToken) {
            return InsertGoogleBigQueryRows(items, cancellationToken);
        }

        private Task InsertGoogleBigQueryRows(IEnumerable<IGoogleBigQueryRow> googleBigQueryRows, CancellationToken cancellationToken) {
            var tasks = new List<Task<BaseResponse<TableDataInsertAllResponse>>>();

            if (googleBigQueryRows != null && googleBigQueryRows.Any()) {

                //items without date
                {
                    var rows = googleBigQueryRows.OfType<IGoogleBigQueryRow>().Where(c => !c.getPartitionDate().HasValue);
                    if (rows.Any()) { tasks.Add(bigQueryService.InsertRowsAsync(null, rows, cancellationToken)); }
                }

                //items with date
                {
                    var groups = googleBigQueryRows.OfType<IGoogleBigQueryRow>().Where(c => c.getPartitionDate().HasValue).GroupBy(c => c.getPartitionDate().Value.Date);
                    foreach (var group in groups) {
                        tasks.Add(bigQueryService.InsertRowsAsync(group.Key, group, cancellationToken));
                    }
                }

                return Task.WhenAll(tasks).ContinueWith(AnalyseReponse);
            }

            return Task.CompletedTask;
        }

        private void AnalyseReponse(Task<BaseResponse<TableDataInsertAllResponse>[]> allTasks) {
            if (allTasks.IsFaulted) {
                throw allTasks.Exception.InnerException;
            } else {

                var qError = allTasks.Result.Where(c => c != null && c.Response == null
                            && (c.ResponseCode == System.Net.HttpStatusCode.BadRequest || c.ResponseCode == System.Net.HttpStatusCode.NotFound));

                if (qError.Any()) {
                    throw new Exception(qError.First().Error.message);
                }

                var errorResponses = allTasks.Result.Where(c => c != null && c.Response != null && c.Response.insertErrors != null && c.Response.insertErrors.Any());

                if (errorResponses.Any()) {
                    var listErrors = from e in errorResponses
                                     from ie in e.Response.insertErrors
                                     from le in ie.errors
                                     select le.message;

                    throw new Exception("BigQuery insert errors", new Exception(string.Join("\n", listErrors)));
                }
            }
        }

        public void Dispose() {
            // Dispose of unmanaged resources.
            items.Clear();
            items = null;
            // Suppress finalization.
            GC.SuppressFinalize(this);
        }

    }
}