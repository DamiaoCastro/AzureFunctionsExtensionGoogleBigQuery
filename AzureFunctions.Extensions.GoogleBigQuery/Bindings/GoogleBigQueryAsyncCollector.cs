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

    
    public class GoogleBigQueryAsyncCollector : ICollector<IGoogleBigQueryRow>, IAsyncCollector<IGoogleBigQueryRow> {

        private readonly ITableData bigQueryService;

        private List<IGoogleBigQueryRow> items = new List<IGoogleBigQueryRow>();

        public GoogleBigQueryAsyncCollector(ITableData bigQueryService) {
            this.bigQueryService = bigQueryService;
        }

        void ICollector<IGoogleBigQueryRow>.Add(IGoogleBigQueryRow item) {
            if (item == null) {
                throw new ArgumentNullException(nameof(item));
            }

            items.Add(item);
        }

        Task IAsyncCollector<IGoogleBigQueryRow>.AddAsync(IGoogleBigQueryRow item, CancellationToken cancellationToken) {
            ((ICollector<IGoogleBigQueryRow>)this).Add(item);

            return Task.CompletedTask;
        }

        Task IAsyncCollector<IGoogleBigQueryRow>.FlushAsync(CancellationToken cancellationToken) {

            var tasks = new List<Task<BaseResponse<TableDataInsertAllResponse>>>();

            if (items.Count > 0) {

                //items without date
                {
                    var rows = items.OfType<IGoogleBigQueryRow>().Where(c => !c.getPartitionDate().HasValue);
                    if (rows.Any()) { tasks.Add(bigQueryService.InsertRowsAsync(null, rows, cancellationToken)); }
                }

                //items with date
                {
                    var groups = items.OfType<IGoogleBigQueryRow>().Where(c => c.getPartitionDate().HasValue).GroupBy(c => c.getPartitionDate().Value.Date);
                    foreach (var group in groups) {
                        tasks.Add(bigQueryService.InsertRowsAsync(group.Key, group, cancellationToken));
                    }
                }

            }

            return Task.WhenAll(tasks)
                .ContinueWith((Task<BaseResponse<TableDataInsertAllResponse>[]> allTasks) => {
                    if (allTasks.IsFaulted) {
                        throw allTasks.Exception.InnerException;
                    } else {

                        var qBadRequest = allTasks.Result.Where(c => c != null && c.Response == null && c.ResponseCode == System.Net.HttpStatusCode.BadRequest);
                        if (qBadRequest.Any()) {
                            throw new Exception(qBadRequest.First().Error.message);
                        }

                        var errorResponses = allTasks.Result.Where(c => c != null && c.Response.insertErrors != null && c.Response.insertErrors.Any());

                        if (errorResponses.Any()) {
                            var listErrors = from e in errorResponses
                                             from ie in e.Response.insertErrors
                                             from le in ie.errors
                                             select le.message;

                            throw new Exception("BigQuery insert errors", new Exception(string.Join("\n", listErrors)));
                        }
                    }
                });

        }

    }

}