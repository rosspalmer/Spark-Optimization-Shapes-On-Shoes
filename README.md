# Spark Optimization: Shapes On Shoes

Example optimization of a long-running Spark application on shoe ratings data.

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

## Map on Group Design

The core challenge of this problem is that we need to perform multiple types
of aggregate operations on a single `customer_id` group by. For example, the
best / worst shoe fields will need the original full row of un-aggregated data.

There are two different paths to take to handle this complexity:

1. Maintain the objects (typing) through the entire group-by
2. Utilize SparkSQL native `agg` functions and joins to build a matching schema

While **Option 2** may be very efficient if done correctly, the different dimensions
of the final dataset require multiple operations, a lengthy `agg(f...)`statement, 
and would be liable to have excess shuffles.

**Option 1** can be achieved easily using the `.groupByKey.mapGroups` method, 
which will retain typing as it passes to the map function a tuple with 
each `customer_id` paired with an iterator of their `CustomerPurchase` set.

Grouping into a map can be a quick way to retain typing and work with the
original data, but similar to `.map()` vs a Spark UDF, it is not the most
efficient available option.

```scala
object ShapesOnShoesV1App extends ShapesOnShoesApp {

  def transformPurchases(purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = {

    import spark.implicits._

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
          name_variants = if (names.length > 1) names.map(_._1).toSet else Set.empty,
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

### Design Results

TODO At scale tests

## Aggregator Design

As we will soon find out, using the Spark SQL `Aggregator` will lengthen our code 
and is seemingly more complicated. While it does make the query on the input data
a typed action and brief, we have to a) create a near duplicate data class and
b) define explicitly each stage in the aggregation. This additional overhead on
the code side is worth the performance benefits based on two factors:

1. Using an intermediary "buffer class" allows the aggregation to drop data
when possible thereby minimizing the size of data shuffled in the group by 
2. The `Aggregator` creates a Spark UDAF (TODO?) which is vectorized and
lets the Spark Catalyst engine optimize as much as possible. (TODO?)

```scala
trait V2 {

  def transformPurchases(purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = {

    import purchases.sparkSession.implicits._

    val aggFunction = new PurchaseAggregator().toColumn

    purchases.groupByKey(_.customer_id)
             .agg(aggFunction)
             .select($"_2".as[CustomerSummary])

  }

}
```

TODO

```scala
private def mergeBuffers(a: SummaryBuffer, b: SummaryBuffer): SummaryBuffer = {

    // Merge name counts by adding `a` map to `b` map plus `a` lookup counts
    val mergedNameCount = a.nameCount + b.nameCount.map {
      case (name, count) => (name, a.nameCount.getOrElse(name, 0) + count)
    }

    // First purchase is min of two dates
    val firstPurchase: Date = Seq(a.firstPurchase, b.firstPurchase).min

    // Total count and average price can be calculated
    val totalCount = a.totalCount + b.totalCount
    val averagePrice = (a.totalCount * a.averagePrice + b.totalCount * b.averagePrice) / totalCount

    // Only keep circle data if there are no star shoes
    val circleLoverData: Option[Seq[String]] = {
      if (a.star.isDefined || b.star.isDefined) {
        None
      } else {
        (a.circles, b.circles) match {
          case (Some(c1), Some(c2)) => Some(c1 ++ c2)
          case (Some(c), None) => Some(c)
          case (None, Some(c)) => Some(c)
          case _ => None
        }
      }
    }

    SummaryBuffer(

      customerId = a.customerId,
      nameCount = mergedNameCount,
      firstPurchase = firstPurchase,
      totalCount = totalCount,
      averagePrice = averagePrice,

      // Note, merge function is biased towards `b` side
      best = if (a.best.shoe_rating > b.best.shoe_rating) a.best else b.best,
      worst = if (a.worst.shoe_rating < b.worst.shoe_rating) a.worst else b.worst,
      star = (a.star, b.star) match {
        case (Some(aStar), Some(bStar)) => if (aStar.shoe_rating > bStar.shoe_rating) Some(aStar) else Some(bStar)
        case (Some(star), None) => Some(star)
        case (None, Some(star)) => Some(star)
        case _ => None
      },

      circles = circleLoverData

    )

  }
```