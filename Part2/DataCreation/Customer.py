import names
import pandas as pd
import numpy as np
import random

def generate_data(size):
    Customer=[]
    for i in range(size):
        Customer.append([i+1,names.get_full_name(),random.randint(18,100),random.randint(1,500),random.randint(100,10000000)])
    
    Customer=pd.DataFrame(Customer,columns=["id","Name","Age","CountryCode","Salary"])
    Customer.to_csv("Customer.csv",index=False)
    
def main():
    generate_data(50)
    
    
main()
 
