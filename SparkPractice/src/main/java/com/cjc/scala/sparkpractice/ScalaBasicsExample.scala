package com.cjc.scala.sparkpractice

class ScalaBasicsExample {
  
   
    def show() {  
       var myList = Array(10.1,20.2,30.3,40.4,50.5)
       
       // Print all the array elements
       
       for( x <- myList)
       {
         println(x)
       }
       
       // Summing all elements
       
       var total = 0.0;
       for ( i <- 0 to (myList.length - 1)) 
       {
          total = total + myList(i);
       }
       println("Total is " + total);
       
       // Finding the largest element
       
       var max = myList(0);
       for(i <- 0 to (myList.length - 1))
       {
         if (myList(i) > max)
           max = myList(i)
       }
       println("Largest element is : "+max)
       
       // Create Array with Range
       var myList1 = Range(10, 20, 2)
       var myList2 = Range(10,20)
       // Print all the elements in the array
       for(x<-myList1)
       {
         print(" "+x)
       }
       println()
       for(x<-myList2)
       {
         print(" "+x)
       }
       
    }  
}