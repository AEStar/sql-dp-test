package examples

import com.uber.engsec.dp.analysis.differential_privacy.RestrictedSensitivityAnalysis
import com.uber.engsec.dp.analysis.histogram.HistogramAnalysis
import com.uber.engsec.dp.rewriting.differential_privacy._
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.QueryParser
import com.uber.engsec.dp.util.ElasticSensitivity



/** A simple example demonstrating query rewriting for differential privacy.
  */
object QueryRewritingExample extends App {
  // Use the table schemas and metadata defined by the test classes
  System.setProperty("schema.config.path", "src/test/resources/schema.yaml")
  val database = Schema.getDatabase("test")

  // privacy budget
  val EPSILON = 0.1
  // delta parameter: use 1/n^2, with n = 100000
  val DELTA = 1 / (math.pow(100000,2))

  // Helper function to print queries with indentation.
  def printQuery(query: String) = println(s"\n  " + query.replaceAll("\\n", s"\n  ") + "\n")

  def elasticSensitivityExample() = {
    println("*** Elastic sensitivity example ***")

    // Example query: How many US customers ordered product #1?
    val query = """
      |SELECT COUNT(*) FROM orders
      |JOIN customers ON orders.customer_id = customers.customer_id
      |WHERE orders.product_id = 1 AND customers.address LIKE '%United States%'"""
      .stripMargin.stripPrefix("\n")

    // Print the example query and privacy budget
    val root = QueryParser.parseToRelTree(query, database)
    println("Original query:")
    printQuery(query)
    println(s"> Epsilon: $EPSILON")

    // Compute mechanism parameter values from the query. Note the rewriter does this automatically; here we calculate
    // the values manually so we can print them.
    val elasticSensitivity = ElasticSensitivity.smoothElasticSensitivity(root, database, 0, EPSILON, DELTA)
    println(s"> Elastic sensitivity of this query: $elasticSensitivity")
    println(s"> Required scale of Laplace noise: 2 * $elasticSensitivity / $EPSILON = ${2 * elasticSensitivity/EPSILON}")

    // Rewrite the original query to enforce differential privacy using Elastic Sensitivity.
    println("\nRewritten query:")
    val config = new ElasticSensitivityConfig(EPSILON, DELTA, database)
    val rewrittenQuery = new ElasticSensitivityRewriter(config).run(query)
    printQuery(rewrittenQuery.toSql())
  }

  def sampleAndAggregateExample() = {
    println("*** Sample and aggregate example ***")
    /** The lambda parameter is an upper bound for aggregated results within each partition. The algorithm clips values
     * above this threshold. While there is no definitive rule for selecting an optimal value of lambda, the general
     * goal is to use a value that is moderately conservative, so few values are clipped but no so great as to produce
     * a disproportionately large bin that skews the quartile estimation in Step #2.
     */
    val LAMBDA = 2.0

    // Example query: What is the average cost of orders for product 1?
    val query = """
      |SELECT AVG(order_cost) FROM orders
      |WHERE product_id = 1"""
      .stripMargin.stripPrefix("\n")

    // Print the example query and privacy budget
    val root = QueryParser.parseToRelTree(query, database)
    println("Original query:")
    printQuery(query)
    println(s"> Epsilon: $EPSILON")

    // Compute mechanism parameter values from the query. Note the rewriter does this automatically; here we calculate
    // the values manually so we can print them.
    val analysisResults = new HistogramAnalysis().run(root, database).colFacts.head
    println(s"> Aggregation function applied: ${analysisResults.outermostAggregation}")
    val tableName = analysisResults.references.head.table
    val approxRowCount = Schema.getTableProperties(database, tableName)("approxRowCount").toLong

    println(s"> Table being queried: $tableName")
    println(s"> Approximate cardinality of table '$tableName': $approxRowCount")
    println(s"> Number of partitions (default heuristic): $approxRowCount ^ 0.4 = ${math.floor(math.pow(approxRowCount, 0.4)).toInt}")
    println(s"> Lambda: $LAMBDA")

    // Rewrite the original query to enforce differential privacy using Sample and Aggregate.
    println("\nRewritten query:")
    val config = new SampleAndAggregateConfig(EPSILON, LAMBDA, database)
    val rewrittenQuery = new SampleAndAggregateRewriter(config).run(query)
    printQuery(rewrittenQuery.toSql())
  }

  def restrictedSensitivityExample() = {
    println("*** Restricted sensitivity example ***")

    val query = """
          |SELECT COUNT(*) FROM orders
      	 |JOIN customers ON orders.customer_id = customers.customer_id
      	 |WHERE orders.product_id = 1 AND customers.address LIKE '%United States%'"""
      .stripMargin.stripPrefix("\n")

    //Set example query and privacy budget
    val root = QueryParser.parseToRelTree(query, database)
    println("Original query:")
    printQuery(query)
    println(s"> Epsilon: $EPSILON")

    //Compute mechanism parameter values from query
    val restrictedSensitivity = new RestrictedSensitivityAnalysis().run(root, database).colFacts(0).sensitivity.get
    println(s"> Required scale of Laplace noise: $restrictedSensitivity / $EPSILON = ${restrictedSensitivity/EPSILON}")

    //Rewrite the original query to enforce dp
    val config = new RestrictedSensitivityConfig(EPSILON, database)
    val rewrittenQuery = new RestrictedSensitivityRewriter(config).run(query)
    printQuery(rewrittenQuery.toSql())
  }

  def wPINQExample() = {
    println("*** WPINQ example ***")

    val query = """
                  |SELECT COUNT(*) FROM orders
                  |JOIN customers ON orders.customer_id = customers.customer_id
                  |WHERE orders.product_id = 1
      	 """
      .stripMargin.stripPrefix("\n")

    println("Original query:")
    printQuery(query)

    //Rewrite the original query to enforce dp
    val config = new WPINQConfig(EPSILON, database)
    val rewrittenQuery = new WPINQRewriter(config).run(query)
    printQuery(rewrittenQuery.toSql())
  }

  elasticSensitivityExample()
  sampleAndAggregateExample()
  restrictedSensitivityExample()
  wPINQExample()
}
