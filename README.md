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

## Initial Design

The core challenge of this problem is that we need to perform multiple types
of aggregate operations on a single `customer_id` group by. For example, the
best / worst shoe fields will need the original full row of un-aggregated data.

There are two different paths to take to handle this complexity:

1. Maintain the objects (typing) through the entire group-by
2. Utilize SparkSQL native `agg` functions and joins to build a matching schema

While option 2 may be very efficient if done correctly, the different dimensions
of the final dataset require multiple operations, long and hard to read `agg(f...)`
statements, and is liable to have excess shuffles.

Option 1 can be achieved easily using the `.groupByKey.mapGroups` method, 
which will retain typing as it passes to the map function a tuple with 
each `customer_id` paired with an iterator of their `CustomerPurchase` set.

This can be a quick way to retain typing through a map, but similar to
`.map()` vs a Spark UDF, there are not as efficient.

```scala
// TODO
```
