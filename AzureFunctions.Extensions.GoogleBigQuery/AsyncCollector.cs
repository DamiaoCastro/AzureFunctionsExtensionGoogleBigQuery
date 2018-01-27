using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;

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

                var tasks = new List<Task>();

                if (items.Count > 0) {
                    
                    Type itemType = items.First().GetType();

                    var bqService = new BigQueryService(googleBigQueryAttribute, itemType);

                    var groups = items.GroupBy(c => c.Date.Date);
                    foreach (var group in groups) {
                        tasks.Add(bqService.InsertRowsAsync(group.Key, group, cancellationToken));
                    }
                }

                return Task.WhenAll(tasks);
            }

        }

    }

}