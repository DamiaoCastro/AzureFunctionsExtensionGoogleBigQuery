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
                return Task.WhenAll();
            }

            Task IAsyncCollector<GoogleBigQueryRow>.FlushAsync(CancellationToken cancellationToken) {

                var tasks = new List<Task<BaseResponse<TableDataInsertAllResponse>>>();

                if (items.Count > 0) {

                    //Type itemType = items.First().GetType();

                    var bqService = new BigQueryService(googleBigQueryAttribute/*, itemType*/);

                    //items without date
                    {
                        var rows = items.Where(c => !c.__Date.HasValue);
                        tasks.Add(bqService.InsertRowsAsync(null, rows, cancellationToken));
                    }

                    //items with date
                    {
                        var groups = items.Where(c => c.__Date.HasValue).GroupBy(c => c.__Date.Value.Date);
                        foreach (var group in groups) {
                            tasks.Add(bqService.InsertRowsAsync(group.Key, group, cancellationToken));
                        }
                    }

                }

                return Task.WhenAll(tasks)
                    .ContinueWith((allTasks) => {
                        if (allTasks.IsFaulted) {
                            throw allTasks.Exception.InnerException;
                        }
                    });
            }

        }

    }

}