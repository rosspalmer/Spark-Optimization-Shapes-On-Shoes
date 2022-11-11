package com.palmer.data.shoes

import java.sql.Date

case class CustomerPurchase(
    customer_id: Long,
    customer_name: String,
    purchase_date: Date,
    shoe_description: String,
    shoe_price: Double,
    shoe_rating: Double
)

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