﻿using System;

namespace AzureFunctions.Extensions.GoogleBigQuery
{
    public class GoogleBigQueryRow : IGoogleBigQueryRow
    {
        private DateTime? __Date;
        private string __InsertId;

        public GoogleBigQueryRow(DateTime? date, string insertId)
        {
            __Date = date;
            __InsertId = insertId;
        }

        DateTime? IGoogleBigQueryRow.getPartitionDate()
        {
            return __Date;
        }

        string IGoogleBigQueryRow.getInsertId()
        {
            return __InsertId;
        }

    }
}