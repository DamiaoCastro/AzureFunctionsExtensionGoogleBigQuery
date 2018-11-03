using Microsoft.Azure.WebJobs.Description;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    [Binding]
    [AttributeUsage(AttributeTargets.ReturnValue | AttributeTargets.Parameter)]
    public sealed class GoogleBigQueryJobAttribute : GoogleBigQueryBaseAttribute {

        public GoogleBigQueryJobAttribute(string credentialsSettingKey) : base(credentialsSettingKey) {           
        }
        
    }
}