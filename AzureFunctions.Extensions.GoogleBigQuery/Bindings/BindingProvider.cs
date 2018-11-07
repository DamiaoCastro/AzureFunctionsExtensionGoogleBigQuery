using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.WebJobs.Host.Protocols;
using System;
using System.Threading.Tasks;

namespace AzureFunctions.Extensions.GoogleBigQuery.Bindings {
    class BindingProvider : IBindingProvider {

        Task<IBinding> IBindingProvider.TryCreateAsync(BindingProviderContext context) {

            //if (context.Parameter.ParameterType is ICollector<>) 
            {

            }

            return Task.FromResult((IBinding)new GoogleBigQueryAttributeBinding());
        }

    }

    internal class GoogleBigQueryAttributeBinding : IBinding {

        bool IBinding.FromAttribute => true;

        Task<IValueProvider> IBinding.BindAsync(object value, ValueBindingContext context) {
            throw new NotImplementedException();
        }

        Task<IValueProvider> IBinding.BindAsync(BindingContext context) {
            throw new NotImplementedException();
        }

        ParameterDescriptor IBinding.ToParameterDescriptor() {
            return new ParameterDescriptor();
        }

    }

}