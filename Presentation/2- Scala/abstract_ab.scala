// Abstract class definition
abstract class Animal {
    def sound(): String // Abstract method
    def move(): String // Abstract method
}

abstract class Mammal extends Animal {
    // Implementation 'sound' method but leave 'move' abstract
    def sound(): String = "Some Sound"
}

class Cat extends Mammal {
    // Must implement 'move' method since 'Cat' is a concrete class
    def move(): String = "Cat moves"
}

// Usage example
object Main extends App {
    val cat = new Cat()
    println(cat.sound()) // Output: Some Sound
    println(cat.move())  // Output: Cat moves
}