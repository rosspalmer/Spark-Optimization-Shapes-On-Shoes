# Spark Optimization Example: Shapes On Shoes

Spark generally provides multiple ways to achieve a given objective and
often requires problem and path specific tuning. While there are many
guidelines and recommendations available online, it is often important
to perform a detailed review of your specific test case and evaluate
the multi-dimensional qualities of a final "optimal" solution.

The example below is single, moderately complex aggregation operation.
The code for running each option will be available in full and is tested
on a large enough dataset to fill a seven node cluster.

## Problem

Here at _Shapes On Shoes_, a fictional shoe manufacture, customers can create their own custom shoes
by mixing shapes and colors and then publicly rate their purchases on a decimal scale from 0 to 10.
These ratings are loaded en masse into our sample table with the schema below:

```scala
case class CustomerPurchase(
    customer_id: Long,
    customer_name: String,
    purchase_date: java.sql.Date,
    shoe_description: String,
    shoe_price: Double,
    shoe_rating: Double
)
```

Our marketing department wants some specific information on each customer.
They also want to be able to directly load this data into the class below:

```scala
case class CustomerSummary(
    customer_id: Long,
    customer_name: String, // Most seen name in data
    name_variants: Set[String], // Other names if found
    first_purchase_date: java.sql.Date,
    total_purchases: Long,
    average_price: Double,
    best_shoe: CustomerPurchase, // Highest rated purchase
    worst_shoe: CustomerPurchase, // Lowest rated purchase
    best_star_shoe: Option[CustomerPurchase], // Highest rated shoe with `star` shape
    circle_lover_designs: Option[Set[String]] // See * below
)
```

_*It turns out that while most people see "star shoes" as the premiere product
but are aware of a growing "no-star" niche with elaborate circle designs_

After generating and saving this summary dataset, the following
metrics need to be calculated:

- 

## Designs

The core challenge of this problem is that we need to perform multiple types
of aggregate operations on a single `customer_id` group by. For example, the
best / worst shoe fields will need the original full row of un-aggregated data.

There are two different paths to take to handle this complexity:

1. Maintain the objects (typing) through the entire group-by
2. Utilize SparkSQL native `agg` functions and joins to build a matching schema

FIXME
While **Option 2** may be very efficient if done correctly, the different dimensions
of the final dataset require multiple operations, untyped `agg/select`statements, 
and could be liable to have excess shuffles.

**Option 1** can be achieved easily using the `.groupByKey.mapGroups` method, 
which will retain typing as it passes to the map function a tuple with 
each `customer_id` paired with an iterator of their `CustomerPurchase` set.

### Design 1) Group by -> map groups

Grouping into a map can be a quick way to retain typing and work with the
original data, but similar to `.map()` vs a Spark UDF, it is not the most
efficient available option.

```scala
object V1 extends TransformFunction {

  def transformPurchases(purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = {

    import purchases.sparkSession.implicits._
    implicit val dateOrdering = new Ordering[Date] {
      override def compare(x: Date, y: Date): Int = x.compareTo(y)
    }

    // Create anonymous function for resolving group of CustomerPurchase
    // objects by customer ID long into final CustomerSummary object
    val groupFunction: (Long, Iterator[CustomerPurchase]) => CustomerSummary = {

      case (customer_id: Long, purchases: Iterator[CustomerPurchase]) =>

        // Sorting full set of purchases by rating descending for future purposes
        val p = purchases.toSeq.sortBy(_.shoe_rating)

        // Sort names by count in reverse
        val names = p.groupBy(_.customer_name).map {
          case (name, list) => (name, list.length)
        }.toSeq.sortBy(_._2).reverse

        // Avoid running `contains` twice for circle shoe logic
        val starShoe: Option[CustomerPurchase] = p.find(_.shoe_description.contains("star"))

        // Populate summary information using sorted purchases sequence
        CustomerSummary(
          customer_id = customer_id,
          customer_name = names.head._1,
          // Only include lower count names as variants
          name_variants = if (names.length > 1) names.map(_._1).slice(1, names.length).toSet else Set.empty,
          first_purchase_date = p.map(_.purchase_date).min,
          total_purchases = p.length,
          average_price = p.map(_.shoe_price).sum / p.length,
          best_shoe = p.head,
          worst_shoe = p.last,
          best_star_shoe = starShoe,
          circle_lover_designs = if (starShoe.isEmpty) Some(
            p.map(_.shoe_description).filter(_.contains("circle")).toSet
          ) else None
        )

    }

    purchases.groupByKey(_.customer_id).mapGroups(groupFunction)

  }

}
```

### Design 2) Spark SQL Aggregator Extension

As we will soon find out, using the Spark SQL `Aggregator` will lengthen our code 
and is seemingly more complicated. While it does make the query on the input data
a typed action and brief, we have to a) create a near duplicate data class and
b) define explicitly each stage in the aggregation. This additional overhead on
the code side is worth the performance benefits based on two factors:

1. Using an intermediary "buffer class" allows the aggregation to drop data
when possible thereby minimizing the size of data shuffled in the group by 
2. The `Aggregator` is typed and can utilize optimize options available to
the Spark SQL Catalyst engine.

A couple performance notes:

- We may need to balance compute vs memory needs such as maintaining the
full list of `names` resolved at the end instead of more computationally
demanding name count map.
- We want to minimize the creation of objects in our RDDs which will demand
GC time, especially with our use case which hopes to drop input data.
  - [Article: Spark Performance Tuning](https://data-flair.training/blogs/apache-spark-performance-tuning/)



```scala
trait V2 {

  def transformPurchases(purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = {

    import purchases.sparkSession.implicits._

    val aggFunction = new PurchaseAggregator().toColumn

    purchases.groupByKey(_.customer_id)
      .agg(aggFunction.name("summary"))
      .select($"summary".as[CustomerSummary])

  }

}
```

TODO

```scala
FIXME
```

### Design 3) Spark SQL native aggregation

TODO

## Generated Data

The "at scale" testing below utilizes a randomly generated dataset using 
`DatasetGenerator.generatePurchaseDataset` which will create a set of
`CustomerPurchases` with predefined properties intended to emulate more
realistic data.  Here are the properties for the single dataset used here:

- **Total Size:** 85.5 GB
- **Num Files:** 200 files
- **Avg Size:** TODO
- **Number of Customers:** 100 million
- **Avg Shoes per Customer:** 30
- **Pct Circle Lovers:** 25%

