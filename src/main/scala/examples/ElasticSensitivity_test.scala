package examples

import java.sql.{DriverManager, ResultSet}
import java.util.Date

import com.uber.engsec.dp.rewriting.differential_privacy.{ElasticSensitivityConfig, ElasticSensitivityRewriter}
import com.uber.engsec.dp.schema.Schema
import examples.QueryRewritingExample.{DELTA, EPSILON, database}

object ElasticSensitivity_test {
  val conn_str = "jdbc:vertica://127.0.0.1:5433/test"
  classOf[com.vertica.jdbc.Driver]

  // Use the table schemas and metadata defined by the test classes
  System.setProperty("schema.config.path", "src/test/resources/schema.yaml")
  val database = Schema.getDatabase("test")

  // privacy budget
  val EPSILON = 0.1
  // delta parameter: use 1/n^2, with n = 100000
  val DELTA = 1 / (math.pow(100000,2))

  // Helper function to print queries with indentation.
  def printQuery(query: String) = println(s"\n  " + query.replaceAll("\\n", s"\n  ") + "\n")

  def main(args: Array[String]) {

    val conn = DriverManager.getConnection(conn_str, "dbadmin", "zhanxuchang159")

    //original query
    val query = """
                  |SELECT COUNT(*)  FROM orders
                  |JOIN customers ON orders.customer_id = customers.customer_id
                  |WHERE orders.product_id < 300
      	 """
      .stripMargin.stripPrefix("\n")

    println("Original query:")
    printQuery(query)

    // Test ElasticSensitivity Rewritten query
    val config = new ElasticSensitivityConfig(EPSILON, DELTA, database)
    val rewrittenQuery = new ElasticSensitivityRewriter(config).run(query)
    val ElasticSensitivityRewrittenQuery = rewrittenQuery.toSql().replace("RAND","RANDOM")
    printQuery(ElasticSensitivityRewrittenQuery)

    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Original Query
      var start_time1 =new Date().getTime
      (1 to 10).foreach { i =>
        val queryResult = statement.executeQuery(query)
        while(queryResult.next()){
          println(queryResult.getInt(1))
        }
      }
      var end_time1 =new Date().getTime
      println(end_time1-start_time1+"\n")

      // Execute Rewritten Query
      var start_time2 =new Date().getTime
      (1 to 10).foreach { i =>
        val ElasticSensitivityResult = statement.executeQuery(ElasticSensitivityRewrittenQuery)
        while(ElasticSensitivityResult.next()){
          println(ElasticSensitivityResult.getInt(1))
        }
      }
      var end_time2 =new Date().getTime
      println(end_time2-start_time2+"\n")

    }finally {
      conn.close
    }
  }
}
