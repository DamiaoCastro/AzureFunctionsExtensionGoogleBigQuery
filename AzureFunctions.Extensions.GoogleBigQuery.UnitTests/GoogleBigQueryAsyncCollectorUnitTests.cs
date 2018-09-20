using AzureFunctions.Extensions.GoogleBigQuery.Bindings;
using AzureFunctions.Extensions.GoogleBigQuery.Services;
using Microsoft.Azure.WebJobs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TransparentApiClient.Google.BigQuery.V2.Schema;
using TransparentApiClient.Google.Core;

namespace AzureFunctions.Extensions.GoogleBigQuery.UnitTests {

    [TestClass]
    public class GoogleBigQueryAsyncCollectorUnitTests {

        private Mock<IBigQueryService> bigQueryServiceMock = null;
        private ICollector<IGoogleBigQueryRow> objectToTest = null;

        [TestInitialize]
        public void Init() {
            bigQueryServiceMock = new Mock<IBigQueryService>();
            objectToTest = new GoogleBigQueryAsyncCollector(bigQueryServiceMock.Object);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Add_Null_ArgumentNullException() {

            //Arrange

            //Act
            objectToTest.Add(null);

            //Assert

        }

        [TestMethod]
        public void Add_RowNullParameters_Success() {

            //Arrange

            //Act
            objectToTest.Add(new GoogleBigQueryRow(null, null));

            //Assert

        }

        [TestMethod]
        public async Task Add_RowDateUtcNow_Success() {

            //Arrange
            var date = new DateTime(2010, 12, 10, 22, 54, 12);
            var row = new GoogleBigQueryRow(date, null);

            var baseResponse = new BaseResponse<TableDataInsertAllResponse>(
                true, 
                new TableDataInsertAllResponse() {
                    insertErrors = null,
                    kind = "kind"
                }, 
                HttpStatusCode.OK, null, null
                );

            DateTime? testDate = null;

            bigQueryServiceMock
                .Setup(m => m.InsertRowsAsync(It.IsAny<DateTime?>(), It.IsAny<IEnumerable<IGoogleBigQueryRow>>(), It.IsAny<CancellationToken>()))
                .Callback(
                    (DateTime? date1, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) => {
                        testDate = date1;

                        //return 1;
                    })
                .ReturnsAsync(baseResponse)
                ;

            //Act
            objectToTest.Add(row);
            await ((IAsyncCollector<IGoogleBigQueryRow>)objectToTest).FlushAsync();

            //Assert
            Assert.AreEqual(date.Date, testDate);

        }

    }
}