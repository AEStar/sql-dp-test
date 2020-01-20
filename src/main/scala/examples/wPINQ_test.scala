package examples

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Date

import com.uber.engsec.dp.rewriting.differential_privacy.{WPINQConfig, WPINQRewriter}
import com.uber.engsec.dp.schema.Schema

object wPINQ_test {

  val conn_str = "jdbc:postgresql://localhost:5432/test"
  classOf[org.postgresql.Driver]

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

    val conn = DriverManager.getConnection(conn_str, "postgres", "zhanxuchang159")

    //original query
    val query = """
                  |SELECT COUNT(*) FROM orders
                  |JOIN customers ON orders.customer_id = customers.customer_id
                  |WHERE orders.product_id = 1
      	 """
      .stripMargin.stripPrefix("\n")

    println("Original query:")
    printQuery(query)

    // Test WPINQ Rewritten query
    val config = new WPINQConfig(EPSILON, database)
    val wPINQRewrittenQueryDemo = new WPINQRewriter(config).run(query)
    val wPINQRewrittenQuery = wPINQRewrittenQueryDemo.toSql().replace("RAND","RANDOM")
    printQuery(wPINQRewrittenQuery)

    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Original Query
      var start_time1 =new Date().getTime
      (1 to 10).foreach { i =>
        val queryResult = statement.executeQuery(query)
      }
      var end_time1 =new Date().getTime
      println(end_time1-start_time1)

      // Execute Rewritten Query
      var start_time2 =new Date().getTime
      (1 to 10).foreach { i =>
        val wPINQueryResult = statement.executeQuery(wPINQRewrittenQuery)
      }
      var end_time2 =new Date().getTime
      println(end_time2-start_time2)

    }finally {
      conn.close
    }
  }
}
