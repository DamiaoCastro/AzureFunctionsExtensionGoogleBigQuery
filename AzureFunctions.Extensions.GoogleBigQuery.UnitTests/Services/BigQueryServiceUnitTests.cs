using AzureFunctions.Extensions.GoogleBigQuery.Services;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AzureFunctions.Extensions.GoogleBigQuery.UnitTests.Services {

    [TestClass]
    public class BigQueryServiceUnitTests {

        [TestMethod]
        public async Task InsertRowsAsync_NullRows_Success() {

            //arrange
            var googleBigQueryAttribute = new GoogleBigQueryCollectorAttribute("credentialsSettingKey", "projectId", "datasetId", "tableId");
            var tableDataClient = new Mock<ITabledata>();
            IBigQueryService objectToTest = new BigQueryService(googleBigQueryAttribute, tableDataClient.Object);

            //act
            var response = await objectToTest.InsertRowsAsync(null, null, CancellationToken.None);

            //assert
            Assert.IsNull(response);

        }

        [TestMethod]
        public async Task InsertRowsAsync_NoRows_Success() {

            //arrange
            var googleBigQueryAttribute = new GoogleBigQueryCollectorAttribute("credentialsSettingKey", "projectId", "datasetId", "tableId");
            var tableDataClient = new Mock<ITabledata>();
            IBigQueryService objectToTest = new BigQueryService(googleBigQueryAttribute, tableDataClient.Object);
            var rows = new List<IGoogleBigQueryRow>();

            //act
            var response = await objectToTest.InsertRowsAsync(null, rows, CancellationToken.None);

            //assert
            Assert.IsNull(response);

        }

        [TestMethod]
        public async Task InsertRowsAsync_OneRowNoDate_Success() {

            //arrange
            var googleBigQueryAttribute = new GoogleBigQueryCollectorAttribute("credentialsSettingKey", "projectId", "datasetId", "tableId");
            var tabledataMock = new Mock<ITabledata>();
            int countInsertAllAsync = 0;
            tabledataMock
                .Setup(c => c.InsertAllAsync(
                    It.Is<string>(t => t == "datasetId"),
                    It.Is<string>(t => t == "projectId"),
                    It.Is<string>(t => t == "tableId"),
                    It.IsAny<TableDataInsertAllRequest>(),
                    It.IsAny<JsonSerializerSettings>(),
                    It.IsAny<CancellationToken>()
                    ))
                .Callback(() => { countInsertAllAsync++; })
                .ReturnsAsync(new TransparentApiClient.Google.Core.BaseResponse<TableDataInsertAllResponse>());

            IBigQueryService objectToTest = new BigQueryService(googleBigQueryAttribute, tabledataMock.Object);
            var rows = new List<IGoogleBigQueryRow>() {
                new GoogleBigQueryRow(null, null)
            };

            //act
            var response = await objectToTest.InsertRowsAsync(null, rows, CancellationToken.None);

            //assert
            Assert.IsNotNull(response);
            Assert.AreEqual(1, countInsertAllAsync);

        }

        [TestMethod]
        public async Task InsertRowsAsync_OneRowWithDate_Success() {

            //arrange
            var googleBigQueryAttribute = new GoogleBigQueryCollectorAttribute("credentialsSettingKey", "projectId", "datasetId", "tableId");
            var tabledataMock = new Mock<ITabledata>();
            int countInsertAllAsync = 0;
            tabledataMock
                .Setup(c => c.InsertAllAsync(
                    It.Is<string>(t => t == "datasetId"),
                    It.Is<string>(t => t == "projectId"),
                    It.Is<string>(t => t == "tableId$20121210"),
                    It.IsAny<TableDataInsertAllRequest>(),
                    It.IsAny<JsonSerializerSettings>(),
                    It.IsAny<CancellationToken>()
                    ))
                .Callback(() => { countInsertAllAsync++; })
                .ReturnsAsync(new TransparentApiClient.Google.Core.BaseResponse<TableDataInsertAllResponse>());

            IBigQueryService objectToTest = new BigQueryService(googleBigQueryAttribute, tabledataMock.Object);
            var rows = new List<IGoogleBigQueryRow>() {
                new GoogleBigQueryRow(null, null)
            };

            //act
            var response = await objectToTest.InsertRowsAsync(new DateTime(2012, 12, 10, 1, 2, 3), rows, CancellationToken.None);

            //assert
            Assert.IsNotNull(response);
            Assert.AreEqual(1, countInsertAllAsync);

        }

    }
}