// Define a trait for basic animal behavior
trait Animal {
    def name: String
    def makeSound(): String
}

// Define another trait for pet-specific behavior
trait Pet {
    def play(): String
}

// Create a class that implements only Animal trait
class Lion(val name: String) extends Animal {
    def makeSound(): String = "Roar!"
}

// Create a class that implements multiple traits
class Dog(val name: String) extends Animal with Pet {
    def makeSound(): String = "Woof!"
    def play(): String = s"$name is playing fetch!"
}

// Main object to test the traits
object TraitsExample extends App {
    val dog = new Dog("Buddy")
    val lion = new Lion("Simba")
    
    println(lion.makeSound()) // Outputs: Roar!
    println(dog.makeSound())  // Outputs: Woof!
    println(dog.play())       // Outputs: Buddy is playing fetch!
}