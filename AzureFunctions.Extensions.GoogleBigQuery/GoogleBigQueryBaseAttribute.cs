using Microsoft.Azure.WebJobs.Description;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {
    
    [Binding]
    [AttributeUsage(AttributeTargets.ReturnValue | AttributeTargets.Parameter)]
    public abstract class GoogleBigQueryBaseAttribute : Attribute {

        public GoogleBigQueryBaseAttribute(string credentialsSettingKey) {
            if (string.IsNullOrWhiteSpace(credentialsSettingKey)) { throw new ArgumentNullException(nameof(credentialsSettingKey)); }

            CredentialsSettingKey = credentialsSettingKey;
        }

        public string CredentialsSettingKey { get; }

    }
}