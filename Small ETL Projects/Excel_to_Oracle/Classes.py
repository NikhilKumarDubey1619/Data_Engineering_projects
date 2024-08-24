
class Vehicle:
    color = "white"

    #Constructors which are called when an object is created
    def __init__(self, max_speed, mileage):
        self.max_speed = max_speed
        self.mileage = mileage
        self.seating_capacity = None

    #Methods of the class
    def assign_seating_capacity(self, seating_capacity):
        self.seating_capacity = seating_capacity

    def display_properties(self):
        print("Properties of the Vehicle:")
        print("Color:", self.color)
        print("Maximum Speed:", self.max_speed)
        print("Mileage:", self.mileage)
        print("Seating Capacity:", self.seating_capacity)

#Created one object of the class Vehicle
Vehicle1 = Vehicle(200,30)
Vehicle1.assign_seating_capacity(5)
Vehicle1.display_properties()
