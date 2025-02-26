
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}
import scala.io.Source


object json_to_scala_faltten {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("json-to-parquet")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val nestedJSON2 ="""{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}"""

    val nestedJSON ="""{
                   "Total_value": 3,
                   "Topic": "Example",
                   "values": [
                              {
                                "value1": "#example1",
                                "points": [
                                           [
                                           "123",
                                           "156"
                                          ]
                                    ],
                                "properties": {
                                 "date": "12-04-19",
                                 "model": "Model example 1"
                                    }
                                 },
                               {"value2": "#example2",
                                "points": [
                                           [
                                           "124",
                                           "157"
                                          ]
                                    ],
                                "properties": {
                                 "date": "12-05-19",
                                 "model": "Model example 2"
                                    }
                                 }
                              ]
                       }"""

    val flattenDF = spark.read.json(spark.createDataset(nestedJSON :: Nil))

    def flattenDataframe(df: DataFrame): DataFrame = {
      val fields = df.schema.fields
      val fieldNames = fields.map(x => x.name)

      for (i <- fields.indices) {
        val field = fields(i)
        val fieldtype = field.dataType
        val fieldName = field.name

        fieldtype match {
          case aType: ArrayType =>
            val firstFieldName = fieldName
            val fieldNamesExcludingArrayType = fieldNames.filter(_ != firstFieldName)
            val explodeFieldNames = fieldNamesExcludingArrayType ++ Array(s"explode_outer($firstFieldName) as $firstFieldName")
            val explodedDf = df.selectExpr(explodeFieldNames: _*)
            return flattenDataframe(explodedDf)

          case sType: StructType =>
            val childFieldnames = sType.fieldNames.map(childname => fieldName + "." + childname)
            val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
            val renamedcols = newfieldNames
              .map(x => (col(x.toString())
                .as(x.toString()
                  .replace(".", "_")
                  .replace("$", "_")
                  .replace("__", "_")
                  .replace(" ", "")
                  .replace("-", ""))))
            val explodedf = df.select(renamedcols: _*)
            return flattenDataframe(explodedf)
          case _ =>
        }
      }
      df
    }
    val FDF = flattenDataframe(flattenDF)
    FDF.show()
    //FDF.write.format("formatType").save("/path/filename")
  }

}