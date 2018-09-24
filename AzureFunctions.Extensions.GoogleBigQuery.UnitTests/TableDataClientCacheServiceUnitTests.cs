using AzureFunctions.Extensions.GoogleBigQuery.Services;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace AzureFunctions.Extensions.GoogleBigQuery.UnitTests {

    [TestClass]
    public class TableDataClientCacheServiceUnitTests {

        [Ignore]
        [TestMethod]
        public void GetTabledataClient_xx_Success() {

            //arrange
            ITableDataClientCacheService objectToTest = new TableDataClientCacheService();
            var googleBigQueryAttribute = new GoogleBigQueryAttribute("credentialsFileName", "projectId", "datasetId", "tableId");

            //act
            var response = objectToTest.GetTabledataClient(googleBigQueryAttribute);

            //assert
            Assert.IsNotNull(response);

        }

    }
}
