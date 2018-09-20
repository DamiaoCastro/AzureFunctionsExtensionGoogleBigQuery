using System;

namespace AzureFunctions.Extensions.GoogleBigQuery
{
    public interface IGoogleBigQueryRow
    {

        DateTime? getPartitionDate();
        string getInsertId();

    }
}