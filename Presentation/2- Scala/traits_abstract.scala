abstract class Animal(val name: String) {  
  def speak(): String  
}  

trait Swimmer {  
  def swim(): String = "I can swim!"  
}  

class Fish(name: String) extends Animal(name) with Swimmer {  
  def speak(): String = "Glub!"  
}  

object Main extends App {
    val nemo = new Fish("Nemo")
    println(nemo.name)    // Prints: Nemo
    println(nemo.speak)   // Prints: Glub!
    println(nemo.swim)    // Prints: I can swim!
}
