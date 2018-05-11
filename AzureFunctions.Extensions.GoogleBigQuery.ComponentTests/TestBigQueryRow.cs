using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace AzureFunctions.Extensions.GoogleBigQuery.ComponentTests {
    public class TestBigQueryRow : GoogleBigQueryRow {

        public TestBigQueryRow(DateTime date, string insertId) : base(date, insertId) { }

        //bool
        [Description("my description for this column")]
        [Column, Required]
        public bool Bool { get; set; }
        [Column] public System.Boolean Boolean { get; set; }
        [Column] public bool? BoolNullable { get; set; }
        [Column] public System.Boolean? BooleanNullable { get; set; }
        [Column] public IEnumerable<bool> BoolEnumerable { get; set; }
        [Column] public IEnumerable<System.Boolean> BooleanEnumerable { get; set; }

        //byte
        [Column] public byte Byte1 { get; set; }
        [Column] public System.Byte Byte2 { get; set; }
        [Column] public byte? Byte1Nullable { get; set; }
        [Column] public System.Byte? Byte2Nullable { get; set; }
        [Column] public IEnumerable<byte> Byte1Enumerable { get; set; }
        [Column] public IEnumerable<System.Byte> Byte2Enumerable { get; set; }
        [Column] public byte[] Byte1Array { get; set; }

        //char
        [Column] public System.Char Char { get; set; }
        [Column] public System.Char? CharNullable { get; set; }
        [Column] public IEnumerable<System.Char> CharEnumerable { get; set; }

        //decimal
        [Column] public System.Decimal Decimal { get; set; }
        [Column] public System.Decimal? DecimalNullable { get; set; }
        [Column] public IEnumerable<System.Decimal> DecimalEnumerable { get; set; }

        //date
        [Column] public System.DateTime DateTime { get; set; }
        [Column] public System.DateTimeOffset DateTimeOffset { get; set; }
        [Column] public System.DateTime? DateTimeNullable { get; set; }
        [Column] public System.DateTimeOffset? DateTimeOffsetNullable { get; set; }
        [Column] public IEnumerable<System.DateTime> DateTimeEnumerable { get; set; }
        [Column] public IEnumerable<System.DateTimeOffset> DateTimeOffsetEnumerable { get; set; }

        //double
        [Column] public double Double1 { get; set; }
        [Column] public System.Double Double2 { get; set; }
        [Column] public double? Double1Nullable { get; set; }
        [Column] public System.Double? Double2Nullable { get; set; }
        [Column] public IEnumerable<double> Double1Enumerable { get; set; }
        [Column] public IEnumerable<System.Double> Double2Enumerable { get; set; }

        //float
        [Column] public float Float { get; set; }
        [Column] public System.Single Single { get; set; }
        [Column] public float? FloatNullable { get; set; }
        [Column] public System.Single? SingleNullable { get; set; }
        [Column] public IEnumerable<float> FloatEnumerable { get; set; }
        [Column] public IEnumerable<System.Single> SingleEnumerable { get; set; }

        //guid
        [Column] public System.Guid Guid { get; set; }
        [Column] public System.Guid? GuidNullable { get; set; }
        [Column] public IEnumerable<System.Guid> GuidEnumerable { get; set; }

        //integer
        [Column] public int Int { get; set; }
        [Column] public System.Int16 Int16 { get; set; }
        [Column] public System.Int32 Int32 { get; set; }
        [Column] public System.Int64 Int64 { get; set; }
        [Column] public System.UInt16 UInt16 { get; set; }
        [Column] public System.UInt32 UInt32 { get; set; }
        [Column] public System.UInt64 UInt64 { get; set; }
        [Column] public int? IntNullable { get; set; }
        [Column] public System.Int16? Int16Nullable { get; set; }
        [Column] public System.Int32? Int32Nullable { get; set; }
        [Column] public System.Int64? Int64Nullable { get; set; }
        [Column] public System.UInt16? UInt16Nullable { get; set; }
        [Column] public System.UInt32? UInt32Nullable { get; set; }
        [Column] public System.UInt64? UInt64Nullable { get; set; }
        [Column] public IEnumerable<int> IntEnumerable { get; set; }
        [Column] public IEnumerable<System.Int16> Int16Enumerable { get; set; }
        [Column] public IEnumerable<System.Int32> Int32Enumerable { get; set; }
        [Column] public IEnumerable<System.Int64> Int64Enumerable { get; set; }
        [Column] public IEnumerable<System.UInt16> UInt16Enumerable { get; set; }
        [Column] public IEnumerable<System.UInt32> UInt32Enumerable { get; set; }
        [Column] public IEnumerable<System.UInt64> UInt64Enumerable { get; set; }

        //string
        [Column] public string String1 { get; set; }
        [Column] public String String2 { get; set; }
        [Column] public IEnumerable<string> String1Enumerable { get; set; }
        [Column] public IEnumerable<String> String2Enumerable { get; set; }

        //sub entities
        [Column] public SimpleEntity1 Record1 { get; set; }
        [Column] public IEnumerable<SimpleEntity1> Record2 { get; set; }
        [Column] public SimpleEntity1[] Record3 { get; set; }

        //not valid case
        [Column, Required]
        public int? incongruency { get; set; }

    }

    public class SimpleEntity1 {

        [Column] public int MySubProperty1 { get; set; }
        [Column] public string MySubProperty2 { get; set; }

    }

}
