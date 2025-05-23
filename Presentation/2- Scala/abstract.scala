// Abstract class definition
abstract class Shape {
    // Abstract method (no implementation)
    def area(): Double
    
    // Abstract method
    def perimeter(): Double
    
    // Concrete method with implementation
    def displayInfo(): Unit = {
        println(s"Area: ${area()}")
        println(s"Perimeter: ${perimeter()}")
    }
}

// Example of a concrete class extending the abstract class
class Circle(radius: Double) extends Shape {
    def area(): Double = Math.PI * radius * radius
    def perimeter(): Double = 2 * Math.PI * radius
}

// Usage example
object Main extends App {
    val circle = new Circle(5.0)
    circle.displayInfo()
}