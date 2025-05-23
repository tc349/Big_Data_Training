trait A {
    def act(): Unit = println("A")
}

trait B {
    def act(): Unit = println("B")
}

class C extends A with B {
    override def act(): Unit = {
        // Call B's implementation
        super[B].act()
    }
}