# Spark Optimization: Shapes On Shoes

Example optimization of long running Spark application on shoe ratings data.

## Problem

At _Shapes On Shoes_, a fictional shoe manufacture, customers can create their own custom shoes
by mixing shapes and colors and then publically rate their purchases on a decimal scale from 0 to 10.
These ratings are loaded en mass into our sample table with the schema below:

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
    total_purchases: Long,
    average_price: Double,
    best_shoe: CustomerPurchase, // Highest rated purchase
    worst_shoe: CustomerPurchase, // Lowest rated purchase
    best_star_shoe: Option[CustomerPurchase], // Highest rated shoe with `star` shape
    circle_lover_designs: Option[Set[String]] // See * below
)
```

*It turns out that while most people see "star shoes" as the pin
