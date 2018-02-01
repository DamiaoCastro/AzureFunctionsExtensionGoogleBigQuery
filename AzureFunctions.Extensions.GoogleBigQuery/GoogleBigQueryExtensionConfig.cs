using Microsoft.Azure.WebJobs.Host.Config;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Azure.WebJobs;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    public class GoogleBigQueryExtensionConfig : IExtensionConfigProvider {

        void IExtensionConfigProvider.Initialize(ExtensionConfigContext context) {
            if (context == null) { throw new ArgumentNullException(nameof(context)); }

            context.AddBindingRule<GoogleBigQueryAttribute>()
                .BindToCollector(c => new AsyncCollector(c));
        }

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

                    var bqService = BigQueryServiceCache.GetPublisherClient(googleBigQueryAttribute, itemType);

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