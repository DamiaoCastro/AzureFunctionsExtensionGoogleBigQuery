using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.BigQuery.V2;
using Microsoft.Azure.WebJobs;

namespace AzureFunctions.Extensions.GoogleBigQuery.Bindings {

    public class GoogleBigQueryAsyncCollector : ICollector<BqRow>, IAsyncCollector<BqRow>, IDisposable {

        private List<BqRow> items = new List<BqRow>();
        private readonly GoogleBigQueryCollectorAttribute googleBigQueryCollectorAttribute;
        private readonly BigQueryClient bigqueryClient;

        public GoogleBigQueryAsyncCollector(GoogleBigQueryCollectorAttribute googleBigQueryCollectorAttribute, BigQueryClient bigqueryClient) {
            this.googleBigQueryCollectorAttribute = googleBigQueryCollectorAttribute;
            this.bigqueryClient = bigqueryClient;
        }

        void ICollector<BqRow>.Add(BqRow item) {
            if (item == null) { throw new ArgumentNullException(nameof(item)); }
            items.Add(item);
        }

        Task IAsyncCollector<BqRow>.AddAsync(BqRow item, CancellationToken cancellationToken) {
            if (item == null) { throw new ArgumentNullException(nameof(item)); }
            items.Add(item);
            return Task.CompletedTask;
        }

        Task IAsyncCollector<BqRow>.FlushAsync(CancellationToken cancellationToken) {
            return InsertGoogleBigQueryRows(items, cancellationToken);
        }

        private Task InsertGoogleBigQueryRows(IEnumerable<BqRow> googleBigQueryRows, CancellationToken cancellationToken) {
            var tasks = new List<Task<BigQueryInsertResults>>();

            if (googleBigQueryRows != null && googleBigQueryRows.Any()) {

                //items without date
                {
                    IEnumerable<BigQueryInsertRow> rows = googleBigQueryRows.Where(c => !c.Date.HasValue).Select(r => r.AsBigQueryInsertRow());

                    if (rows.Any()) {

                        Task<BigQueryInsertResults> t = bigqueryClient
                            .InsertRowsAsync(
                                googleBigQueryCollectorAttribute.DatasetId,
                                googleBigQueryCollectorAttribute.TableId,
                                rows,
                                new InsertOptions() { AllowEmptyInsertIds = true, AllowUnknownFields = true, SuppressInsertErrors = false },
                                cancellationToken
                                );

                        tasks.Add(t);
                    }
                }

                //items with date
                {
                    var groups = googleBigQueryRows.Where(c => c.Date.HasValue).GroupBy(c => c.Date.Value.Date);
                    foreach (var group in groups) {

                        var rows = group.Select(r => r.AsBigQueryInsertRow()); ;

                        Task<BigQueryInsertResults> t = bigqueryClient
                            .InsertRowsAsync(
                                googleBigQueryCollectorAttribute.DatasetId,
                                $"{googleBigQueryCollectorAttribute.TableId}${group.Key:yyyyMMdd}",
                                rows,
                                new InsertOptions() { AllowEmptyInsertIds = true, AllowUnknownFields = true, SuppressInsertErrors = false },
                                cancellationToken
                                );

                    }
                }

                return Task.WhenAll(tasks).ContinueWith(AnalyseReponse);
            }

            return Task.CompletedTask;
        }

        private void AnalyseReponse(Task<BigQueryInsertResults[]> allTasks) {
            if (allTasks.IsFaulted) {
                throw allTasks.Exception.InnerException;
            } else {

                var qError = allTasks.Result.Where(c => c != null && c.Errors.Any());

                if (qError.Any()) {
                    qError.First().ThrowOnAnyError();
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