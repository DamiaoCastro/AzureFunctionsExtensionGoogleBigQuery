using AzureFunctions.Extensions.GoogleBigQuery.Bindings;
using AzureFunctions.Extensions.GoogleBigQuery.Services;
using Microsoft.Azure.WebJobs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TransparentApiClient.Google.BigQuery.V2.Schema;
using TransparentApiClient.Google.Core;

namespace AzureFunctions.Extensions.GoogleBigQuery.UnitTests {

    [TestClass]
    public class GoogleBigQueryAsyncCollectorUnitTests {

        private Mock<ITableData> bigQueryServiceMock = null;
        private ICollector<IGoogleBigQueryRow> objectToTest = null;

        [TestInitialize]
        public void Init() {
            bigQueryServiceMock = new Mock<ITableData>();
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
        public async Task FlushAsync_RowWithoutDayPartition_Success() {

            //Arrange
            DateTime? testDate = null;
            IEnumerable<IGoogleBigQueryRow> testRows = null;
            int countInsertRowsAsync = 0;

            bigQueryServiceMock
                .Setup(m => m.InsertRowsAsync(It.IsAny<DateTime?>(), It.IsAny<IEnumerable<IGoogleBigQueryRow>>(), It.IsAny<CancellationToken>()))
                .Callback(
                    (DateTime? date1, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) => {
                        testDate = date1;
                        testRows = rows;
                        countInsertRowsAsync++;
                    })
                .ReturnsAsync(GetBaseResponse());

            //Act
            objectToTest.Add(new GoogleBigQueryRow(null, null));
            await ((IAsyncCollector<IGoogleBigQueryRow>)objectToTest).FlushAsync();

            //Assert
            Assert.AreEqual(1, countInsertRowsAsync);
            Assert.IsNull(testDate);
            Assert.AreEqual(1, testRows.Count());
            Assert.IsNull(testRows.First().getPartitionDate());
            Assert.IsNull(testRows.First().getInsertId());

        }

        [TestMethod]
        public async Task FlushAsync_RowWithDayPartition_Success() {

            //Arrange
            var date = new DateTime(2010, 12, 10, 22, 54, 12);
            var row = new GoogleBigQueryRow(date, null);

            DateTime? testDate = null;
            IEnumerable<IGoogleBigQueryRow> testRows = null;
            int countInsertRowsAsync = 0;

            bigQueryServiceMock
                .Setup(m => m.InsertRowsAsync(It.IsAny<DateTime?>(), It.IsAny<IEnumerable<IGoogleBigQueryRow>>(), It.IsAny<CancellationToken>()))
                .Callback(
                    (DateTime? date1, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) => {
                        testDate = date1;
                        testRows = rows;
                        countInsertRowsAsync++;
                    })
                .ReturnsAsync(GetBaseResponse());

            //Act
            objectToTest.Add(row);
            await ((IAsyncCollector<IGoogleBigQueryRow>)objectToTest).FlushAsync();

            //Assert
            Assert.AreEqual(1, countInsertRowsAsync);
            Assert.AreEqual(date.Date, testDate);
            Assert.AreEqual(1, testRows.Count());
            Assert.AreEqual(date, testRows.First().getPartitionDate());
            Assert.IsNull(testRows.First().getInsertId());

        }

        [TestMethod]
        public async Task FlushAsync_RowWithDayPartitionAndInsertId_Success() {

            //Arrange
            var testDates = new List<DateTime?>();
            var testRows = new List<IGoogleBigQueryRow>();
            int countInsertRowsAsync = 0;

            bigQueryServiceMock
                .Setup(m => m.InsertRowsAsync(It.IsAny<DateTime?>(), It.IsAny<IEnumerable<IGoogleBigQueryRow>>(), It.IsAny<CancellationToken>()))
                .Callback(
                    (DateTime? date1, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) => {
                        testDates.Add(date1);
                        testRows.AddRange(rows);
                        countInsertRowsAsync++;
                    })
                .ReturnsAsync(GetBaseResponse());

            //Act
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 10, 19, 54, 12), "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 10, 20, 54, 12), "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 10, 21, 54, 12), "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 10, 22, 54, 12), "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 10, 23, 54, 12), "insertId"));
            await ((IAsyncCollector<IGoogleBigQueryRow>)objectToTest).FlushAsync();

            //Assert
            Assert.AreEqual(1, countInsertRowsAsync);
            Assert.AreEqual(1, testDates.Count());
            Assert.AreEqual(new DateTime(2010, 12, 10, 0, 0, 0), testDates.First());
            Assert.AreEqual(5, testRows.Count());
            Assert.IsTrue(testRows.TrueForAll(c => c.getInsertId() == "insertId"));

        }

        [TestMethod]
        public async Task FlushAsync_RowWithoutDayPartitionAndInsertId_Success() {

            //Arrange
            var testDates = new List<DateTime?>();
            var testRows = new List<IGoogleBigQueryRow>();
            int countInsertRowsAsync = 0;

            bigQueryServiceMock
                .Setup(m => m.InsertRowsAsync(It.IsAny<DateTime?>(), It.IsAny<IEnumerable<IGoogleBigQueryRow>>(), It.IsAny<CancellationToken>()))
                .Callback(
                    (DateTime? date1, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) => {
                        testDates.Add(date1);
                        testRows.AddRange(rows);
                        countInsertRowsAsync++;
                    })
                .ReturnsAsync(GetBaseResponse());

            //Act
            objectToTest.Add(new GoogleBigQueryRow(null, "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(null, "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(null, "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(null, "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(null, "insertId"));
            await ((IAsyncCollector<IGoogleBigQueryRow>)objectToTest).FlushAsync();

            //Assert
            Assert.AreEqual(1, countInsertRowsAsync);
            Assert.AreEqual(1, testDates.Count());
            Assert.IsNull(testDates.First());
            Assert.AreEqual(5, testRows.Count());
            Assert.IsTrue(testRows.TrueForAll(c => c.getInsertId() == "insertId"));

        }

        [TestMethod]
        public async Task FlushAsync_RowWithAndWithoutDayPartitionAndInsertId_Success() {

            //Arrange
            var testDates = new List<DateTime?>();
            var testRows = new List<IGoogleBigQueryRow>();
            int countInsertRowsAsync = 0;

            bigQueryServiceMock
                .Setup(m => m.InsertRowsAsync(It.IsAny<DateTime?>(), It.IsAny<IEnumerable<IGoogleBigQueryRow>>(), It.IsAny<CancellationToken>()))
                .Callback(
                    (DateTime? date1, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) => {
                        testDates.Add(date1);
                        testRows.AddRange(rows);
                        countInsertRowsAsync++;
                    })
                .ReturnsAsync(GetBaseResponse());

            //Act
            objectToTest.Add(new GoogleBigQueryRow(null, "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(null, "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 10, 21, 54, 12), "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 10, 22, 54, 12), "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 10, 23, 54, 12), "insertId"));
            await ((IAsyncCollector<IGoogleBigQueryRow>)objectToTest).FlushAsync();

            //Assert
            Assert.AreEqual(2, countInsertRowsAsync);
            Assert.AreEqual(2, testDates.Count());
            Assert.IsNull(testDates.First());
            Assert.AreEqual(new DateTime(2010, 12, 10, 0, 0, 0), testDates.Skip(1).First());
            Assert.AreEqual(5, testRows.Count());
            Assert.IsTrue(testRows.TrueForAll(c => c.getInsertId() == "insertId"));

        }

        [TestMethod]
        public async Task FlushAsync_RowWithAndWithoutDayPartitionDifferentDaysAndInsertId_Success() {

            //Arrange
            var testDates = new List<DateTime?>();
            var testRows = new List<IGoogleBigQueryRow>();
            int countInsertRowsAsync = 0;

            bigQueryServiceMock
                .Setup(m => m.InsertRowsAsync(It.IsAny<DateTime?>(), It.IsAny<IEnumerable<IGoogleBigQueryRow>>(), It.IsAny<CancellationToken>()))
                .Callback(
                    (DateTime? date1, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) => {
                        testDates.Add(date1);
                        testRows.AddRange(rows);
                        countInsertRowsAsync++;
                    })
                .ReturnsAsync(GetBaseResponse());

            //Act
            objectToTest.Add(new GoogleBigQueryRow(null, "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(null, "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 10, 21, 54, 12), "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 11, 22, 54, 12), "insertId"));
            objectToTest.Add(new GoogleBigQueryRow(new DateTime(2010, 12, 12, 23, 54, 12), "insertId"));
            await ((IAsyncCollector<IGoogleBigQueryRow>)objectToTest).FlushAsync();

            //Assert
            Assert.AreEqual(4, countInsertRowsAsync);
            Assert.AreEqual(4, testDates.Count());
            Assert.IsNull(testDates.First());
            Assert.AreEqual(new DateTime(2010, 12, 10, 0, 0, 0), testDates.Skip(1).First());
            Assert.AreEqual(new DateTime(2010, 12, 11, 0, 0, 0), testDates.Skip(2).First());
            Assert.AreEqual(new DateTime(2010, 12, 12, 0, 0, 0), testDates.Skip(3).First());
            Assert.AreEqual(5, testRows.Count());
            Assert.IsTrue(testRows.TrueForAll(c => c.getInsertId() == "insertId"));

        }

        [TestMethod]
        public async Task FlushAsync_RowDateUtcNow_Success() {

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
            IEnumerable<IGoogleBigQueryRow> testRows = null;

            bigQueryServiceMock
                .Setup(m => m.InsertRowsAsync(It.IsAny<DateTime?>(), It.IsAny<IEnumerable<IGoogleBigQueryRow>>(), It.IsAny<CancellationToken>()))
                .Callback(
                    (DateTime? date1, IEnumerable<IGoogleBigQueryRow> rows, CancellationToken cancellationToken) => {
                        testDate = date1;
                        testRows = rows;
                    })
                .ReturnsAsync(baseResponse);

            //Act
            objectToTest.Add(row);
            await ((IAsyncCollector<IGoogleBigQueryRow>)objectToTest).FlushAsync();

            //Assert
            Assert.AreEqual(date.Date, testDate);

        }

        [TestMethod]
        public async Task FlushAsync_InsertRowsAsyncReturnsNull() {

            //Arrange
            var row = new GoogleBigQueryRow(null, null);

            bigQueryServiceMock
                .Setup(m => m.InsertRowsAsync(It.IsAny<DateTime?>(), It.IsAny<IEnumerable<IGoogleBigQueryRow>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((BaseResponse<TableDataInsertAllResponse>)null);

            //Act
            objectToTest.Add(row);
            await ((IAsyncCollector<IGoogleBigQueryRow>)objectToTest).FlushAsync();

            //Assert

        }

        private static BaseResponse<TableDataInsertAllResponse> GetBaseResponse() {
            return new BaseResponse<TableDataInsertAllResponse>(
                true,
                new TableDataInsertAllResponse() {
                    insertErrors = null,
                    kind = "kind"
                },
                HttpStatusCode.OK, null, null
                );
        }

    }
}