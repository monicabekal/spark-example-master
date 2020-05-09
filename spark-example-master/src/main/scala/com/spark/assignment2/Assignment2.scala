package com.spark.assignment2


import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}



object Assignment2 {

  def Problem1(octDF:DataFrame):DataFrame ={
    val eventtype = octDF.select("event_type"). //selecting the field event type
      groupBy("event_type"). //This group by function helps in grouping records with same event type
      count() //this helps in getting count for each of the distinct event types
    return eventtype //displays the result
  }

  def Problem2(octDF:DataFrame):DataFrame={
    val categorycode = octDF.select("category_code"). //helps to select the specific field name
      distinct() //Helps to get the distinct field values
    return categorycode //displays the result
  }

  def Problem3(octDF:DataFrame):String={
    val highproduct = octDF.select(octDF("category_code")). //Helps to select the category code
      sort(desc("price")) // Sorting is done in order to arrange the records based on the price in the descending order

    highproduct.first.toString()// the high record value is picked since it has the highest cost value.
  }

  def Problem4(octDF:DataFrame,novDF:DataFrame): Long = {
    val Octviewed = octDF.filter(octDF("event_type") === "view") //filtering the oct dataframe to get records containing event type = view
    val NovPurchased = novDF.filter(novDF("event_type") === "purchase") //filtering the nov dataframe to get records containing event type = purchase
    val customerreturn = Octviewed.join(NovPurchased,Octviewed("user_id") === NovPurchased("user_id") //joining the two dataframes which are derived above
      && Octviewed("category_code") ===  NovPurchased("category_code")//the conditions for the join are on the userid,category and brand
      && Octviewed("brand") ===  NovPurchased("brand")
      ,"inner").select(Octviewed("user_id")) //the join type is inner and we are selecting just the user id
    customerreturn.count() //doing the count
  }

  def Problem5(octDF:DataFrame,novDF:DataFrame): Boolean = {
    val Novcost = novDF.select("price").//This helps in getting the price
      filter((novDF("category_code") === "electronics.tablet") && (novDF("brand") === "apple")).//making sure the filter is on the category and the brand
      sort(desc("price")).//we are sorting the records based on descending order.
      first() //making sure just one record is chosen.
    val novcost1 = Novcost.toString()//converting it into string value.
    val Octcost = octDF.select("price").//this helps in getting the price
      filter((octDF("category_code") === "electronics.tablet") && (octDF("brand") === "apple")).//making sure the filter is based on category and brand
      sort(desc("price")).//we are sorting the records based on descending order.
      first() //making sure just one record is chosen.
    val octcost1 =  Octcost.toString()//converting it into string value
    novcost1 == octcost1//checking if the cost is the same .
  }

  def Problem6(tecinDF: DataFrame): String = {
    val ps4high = tecinDF.select("Country").// selecting the country field.
      sort(desc("PS4"))//sorting based on descending order of ps4 cost.
    ps4high.first.toString()//restricting just one record.
  }

  def Problem7(novDF:DataFrame,tecinDF:DataFrame):Boolean = {
    val Presentcost = novDF.select("price").//helps in selecting the price
      filter((novDF("category_code") === "computers.notebook") && (novDF("brand") === "apple"))//conditions based on category and brand
      .sort(desc("price")).//sorting in descending order
      first()//picking up the first record.
    val Pastcost = tecinDF.select("MACBook").//selecting macbook cost
      filter(tecinDF("Country") === "USA")//selecting the country as USA
    Presentcost == Pastcost //check if the costs are same.
  }





}


