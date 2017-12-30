using System;
using System.ComponentModel.DataAnnotations.Schema;

namespace AzureFunctions.Extensions.GoogleBigQuery.ComponentTests {
    public class TestBigQueryRow : GoogleBigQueryRow {

        public TestBigQueryRow(DateTime date, string insertId) : base(date, insertId) { }

        //byte
        public System.Byte ByteValue { get; set; }
        public System.Byte? ByteNullableValue { get; set; }

        //integer
        [Column] public int IntValue { get; set; }
        [Column] public System.Int16 Int16Value { get; set; }
        [Column] public System.Int32 Int32Value { get; set; }
        [Column] public System.Int64 Int64Value { get; set; }
        [Column] public System.UInt16 UInt16Value { get; set; }
        [Column] public System.UInt32 UInt32Value { get; set; }
        [Column] public System.UInt64 UInt64Value { get; set; }
        [Column] public int? IntNullableValue { get; set; }

        public float MyProperty { get; set; }
        public double Double1Value { get; set; }
        public System.Double Double2Value { get; set; }
        public System.Boolean Boolean { get; set; }
        public System.Decimal Decimal { get; set; }
        public System.Single Single { get; set; }
        public System.Char Char { get; set; }
        public System.DateTime DateTime { get; set; }
        public System.DateTimeOffset DateTimeOffset { get; set; }

        public String StringValue { get; set; }

        public System.Guid GuidValue { get; set; }


    }
}
