using AzureFunctions.Extensions.GoogleBigQuery.TestsCommon;
using Microsoft.Azure.WebJobs.Host.Config;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AzureFunctions.Extensions.GoogleBigQuery.ComponentTests {
    [TestClass]
    public class BigQueryServiceTests {

        private BigQueryService bigQueryService = null;

        [TestInitialize]
        public void Initialize() {
            bigQueryService = new BigQueryService(new GoogleBigQueryAttribute(null, "damiao-1982", "extensiontest", "table1"), typeof(TestBigQueryRow));
        }

        [TestMethod]
        public async Task InsertRowsAsync_EmptyRow() {

            //Arrange
            TestBigQueryRow testBigQueryRow = new TestBigQueryRow(DateTime.UtcNow, "insertId_1") {
                Double1Enumerable = new double[] { },
                Double2Enumerable = new double[] { },
                String1Enumerable = new string[] { },
                String2Enumerable = new string[] { },
                BooleanEnumerable = new bool[] { },
                BoolEnumerable = new bool[] { },
                Byte1Enumerable = new byte[] { },
                Byte2Enumerable = new byte[] { },
                Byte1Array = new byte[] { },
                CharEnumerable = new char[] { },
                DateTimeEnumerable = new DateTime[] { },
                DateTimeOffsetEnumerable = new DateTimeOffset[] { },
                DecimalEnumerable = new decimal[] { },
                FloatEnumerable = new float[] { },
                GuidEnumerable = new Guid[] { },
                Int16Enumerable = new Int16[] { },
                Int32Enumerable = new Int32[] { },
                Int64Enumerable = new Int64[] { },
                IntEnumerable = new int[] { },
                SingleEnumerable = new Single[] { },
                UInt16Enumerable = new UInt16[] { },
                UInt32Enumerable = new UInt32[] { },
                UInt64Enumerable = new UInt64[] { },


                Record2 = new List<SimpleEntity1> { },
                Record3 = new SimpleEntity1[] { }
            };

            //Act
            await bigQueryService.InsertRowsAsync(testBigQueryRow.Date, new TestBigQueryRow[] { testBigQueryRow }, CancellationToken.None);

            //Assert

        }

        [TestMethod]
        public async Task InsertRowsAsync_WithSomeRecords() {

            //Arrange
            TestBigQueryRow testBigQueryRow = new TestBigQueryRow(DateTime.UtcNow, "insertId_2") {
                Double1Enumerable = new double[] { },
                Double2Enumerable = new double[] { },
                String1Enumerable = new string[] { },
                String2Enumerable = new string[] { },
                BooleanEnumerable = new bool[] { },
                BoolEnumerable = new bool[] { },
                Byte1Enumerable = new byte[] { },
                Byte2Enumerable = new byte[] { },
                Byte1Array = new byte[] { },
                CharEnumerable = new char[] { },
                DateTimeEnumerable = new DateTime[] { },
                DateTimeOffsetEnumerable = new DateTimeOffset[] { },
                DecimalEnumerable = new decimal[] { },
                FloatEnumerable = new float[] { },
                GuidEnumerable = new Guid[] { },
                Int16Enumerable = new Int16[] { },
                Int32Enumerable = new Int32[] { },
                Int64Enumerable = new Int64[] { },
                IntEnumerable = new int[] { },
                SingleEnumerable = new Single[] { },
                UInt16Enumerable = new UInt16[] { },
                UInt32Enumerable = new UInt32[] { },
                UInt64Enumerable = new UInt64[] { },

                Record1 = new SimpleEntity1() { MySubProperty1 = 1, MySubProperty2 = "Record1.MySubProperty2" },
                Record2 = new List<SimpleEntity1> {
                    new SimpleEntity1() { MySubProperty1 = 1, MySubProperty2 = "Record2.MySubProperty2-1" } ,
                    new SimpleEntity1() { MySubProperty1 = 2, MySubProperty2 = "Record2.MySubProperty2-2" },
                    new SimpleEntity1() { MySubProperty1 = 3, MySubProperty2 = "Record2.MySubProperty2-3" }
                },
                Record3 = new SimpleEntity1[] {
                    new SimpleEntity1() { MySubProperty1 = 1, MySubProperty2 = "Record3.MySubProperty3-1" } ,
                    new SimpleEntity1() { MySubProperty1 = 2, MySubProperty2 = "Record3.MySubProperty3-2" }
                }
            };

            //Act
            await bigQueryService.InsertRowsAsync(testBigQueryRow.Date, new TestBigQueryRow[] { testBigQueryRow }, CancellationToken.None);

            //Assert

        }

    }
}