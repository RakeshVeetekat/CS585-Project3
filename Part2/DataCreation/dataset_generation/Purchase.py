import names
import pandas as pd
import numpy as np
import random

def main():
    generate_data(5000000)
    

def generate_data(size):
    Purchase=[]
    TransDesc=["Grocery: Egg Bread Ham Coffee","Grocery: Rice Floor Bread","Clothes: Trousers Glass Hoodie", "Clothes: Jumper Skirt Shirt","Clothes: Watch Scarf Hat","Pet: Food Toy Bed Castle","Children: Toys Boots Jumper","Beauty: Skincare Bodycare Sunscreen Lotion Eyelashes","Beauty: Hair Shampoo Conditioner","Beauty: Cream Body Cream Base Concealer","Home Appliance: Kitchen Bedroom Bathroom","Other: Electronics Tools Gadgets"]
    for i in range(size):
        Purchase.append([i+1,random.randint(1,50000),random.uniform(10,2000),random.randint(1,15),random.choice(TransDesc)])
    
    Purchase=pd.DataFrame(Purchase,columns=["TransID","CustID","TransTotal","TransNumItems","TransDesc"])
    Purchase.to_csv("Purchase.csv",sep=",",index=False)
    
    
main()