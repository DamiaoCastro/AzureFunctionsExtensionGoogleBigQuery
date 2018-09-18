using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using TransparentApiClient.Google.BigQuery.V2.Schema;
using TransparentApiClient.Google.Core;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    public partial class GoogleBigQueryExtensionConfig {
        public class AsyncCollector : ICollector<GoogleBigQueryRow>, IAsyncCollector<GoogleBigQueryRow> {

            private GoogleBigQueryAttribute googleBigQueryAttribute;
            private List<GoogleBigQueryRow> items = new List<GoogleBigQueryRow>();

            public AsyncCollector(GoogleBigQueryAttribute googleBigQueryAttribute) {
                this.googleBigQueryAttribute = googleBigQueryAttribute;
            }

            void ICollector<GoogleBigQueryRow>.Add(GoogleBigQueryRow item) {
                if (item == null) {
                    throw new ArgumentNullException(nameof(item));
                }

                items.Add(item);
            }

            Task IAsyncCollector<GoogleBigQueryRow>.AddAsync(GoogleBigQueryRow item, CancellationToken cancellationToken) {
                if (item == null) {
                    throw new ArgumentNullException(nameof(item));
                }

                items.Add(item);
                return Task.CompletedTask;
            }

            Task IAsyncCollector<GoogleBigQueryRow>.FlushAsync(CancellationToken cancellationToken) {

                var tasks = new List<Task<BaseResponse<TableDataInsertAllResponse>>>();

                if (items.Count > 0) {
                    
                    var bqService = new BigQueryService(googleBigQueryAttribute);

                    //items without date
                    {
                        var rows = items.OfType<IGoogleBigQueryRow>().Where(c => !c.getPartitionDate().HasValue);
                        if (rows.Any()) { tasks.Add(bqService.InsertRowsAsync(null, rows, cancellationToken)); }                        
                    }

                    //items with date
                    {
                        var groups = items.OfType<IGoogleBigQueryRow>().Where(c => c.getPartitionDate().HasValue).GroupBy(c => c.getPartitionDate().Value.Date);
                        foreach (var group in groups) {
                            tasks.Add(bqService.InsertRowsAsync(group.Key, group, cancellationToken));
                        }
                    }

                }

                return Task.WhenAll(tasks)
                    .ContinueWith((Task<BaseResponse<TableDataInsertAllResponse>[]> allTasks) => {
                        if (allTasks.IsFaulted)
                        {
                            throw allTasks.Exception.InnerException;
                        }
                        else {
                            var errorResponses = allTasks.Result.Where(c=> c!= null && c.Response.insertErrors != null && c.Response.insertErrors.Any());

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
}