using AzureFunctions.Extensions.GoogleBigQuery.TestsCommon;
using Microsoft.Azure.WebJobs.Host.Config;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace AzureFunctions.Extensions.GoogleBigQuery.ComponentTests {
    [TestClass]
    public class BigQueryServiceTests {

        private BigQueryService bigQueryService = null;

        [TestInitialize]
        public void Initialize() {
            bigQueryService = new BigQueryService(null, "damiao-1982", "extensiontest", "table1", typeof(TestBigQueryRow));
        }

        [TestMethod]
        public async Task InsertRowsAsync_EmptyTestBigQueryRow() {

            //Arrange
            TestBigQueryRow testBigQueryRow = new TestBigQueryRow(DateTime.UtcNow, "insertId_1") {

            };

            //Act
            await bigQueryService.InsertRowsAsync(testBigQueryRow.Date, new TestBigQueryRow[] { testBigQueryRow }, CancellationToken.None);

            //Assert

        }

    }
}