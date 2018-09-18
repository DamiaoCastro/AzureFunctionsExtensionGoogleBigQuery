using System;

namespace AzureFunctions.Extensions.GoogleBigQuery
{
    public interface IGoogleBigQueryRow
    {

        DateTime? getPartitionDate();
        string getInsertId();

        //private const Attribute[] AcceptedAttibuts = new Attribute[]{
        //    typeof(Newtonsoft.Json.JsonPropertyAttribute),
        //    typeof(System.ComponentModel.DataAnnotations.Schema.ColumnAttribute)
        //};

        //public GoogleBigQueryRow(DateTime? date, string insertId)
        //{
        //    __Date = date;
        //    __InsertId = insertId;

        //    //var properties = this.GetType().GetProperties()
        //    //    .Where(p => p.Name == "test")
        //    //    //.Where(p=> p.CustomAttributes.Contains(AcceptedAttibuts))
        //    //    ;
        //    //foreach (var property in properties)
        //    //{
        //    //    Add(new JProperty(property.Name, property.GetValue(this)));
        //    //}
        //}


        //[Newtonsoft.Json.JsonIgnore]
        //public DateTime? __Date { get; set; }
        //[Newtonsoft.Json.JsonIgnore]
        //public string __InsertId { get; set; }

    }
}