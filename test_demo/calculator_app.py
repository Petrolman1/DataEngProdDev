class Calculator:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def get_sum(self):
        return self.a + self.b
    
    def get_difference(self):
        return self.a- self.b
     
    def get_product(self):
        return self.a * self.b
     
    def get_quotient(self):
        return self.a / self.b    

    
# Example usage
if __name__ == "__main__":
    myCalc = Calculator(2, 4)
    print(myCalc.get_product())
