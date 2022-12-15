import pyodbc
import pandas as pd
from pandas import DataFrame


#   This script traces lot tracked items in Business Central
#   It traces a PO from receipt to all levels of production to final item
#   It can handle multiple BOM / Production order levels
#   1. Get initial list of lots
#   2. Get the production orders that consume them
#   3. Get the list out output lots if any
#   4. repeat 2 to 3 until there are no more production orders.
#   5. Run a full trace on the lot no and production orders
#   6. Output results to csv files in the current working directory

server = 'SERVERNAME' 
database = 'DATABASENAME'
companyname ='MY-COMPANY-NAME$' # BC Company name eg ACME$

## End of input



global po


prodList = []
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database)
cursor = cnxn.cursor()

fullorderList = []
fullLotList = []
curlots = []
'''
Item ledger entry types
 '0'  'Purchase'
 '1'  'Sale'
 '2'  'Positive Adjmt.'
 '3'  'Negative Adjmt.'
 '4'  'Transfer'
 '5'  'Consumption'
 '6'  'Output'
'''

# return all document numbers (production orders) which consume the given lot tracked lot numbers.
getProdOrdersQueryBCDB = f'''select distinct [document no_]  
        FROM [{database}].[dbo].[{companyname}Item Ledger Entry] where  [entry type] = 5 and [Lot No_] ='''

# return all lot numbers recipted into stock from a given PO number
purquery = f'''SELECT DISTINCT [lot No_]
FROM [{database}].[dbo].[{companyname}Item Ledger Entry] i 
left outer join [{database}].[dbo].[{companyname}Purch_ Rcpt_ Header] pr on pr.No_ = i.[Document No_]
where [entry type] = 0 and pr.[Order No_] = ?  '''

# for a given production order number(s) return the lot numbers for their outputted items
outputquery = f'''select distinct [Lot No_]  FROM  [{database}].[dbo].[{companyname}Item Ledger Entry]  
  where  [entry type] = 6 and [Document No_] ='''

# Return all item ledger transactions for a given list of lot nos
fulltraceQueryfromLots = f'''SELECT  [Posting Date] ,[Entry Type]  ,[Item No_]  ,[Description]  ,[Quantity]  ,
[Unit of Measure Code], [Bin Code], 
 [Document No_], [Document Type], 
[Entry No_], [External Document No_]     ,  [Location Code], [Lot No_]
         
FROM [{database}].[dbo].[{companyname}Item Ledger Entry] where [Lot No_] in (%s) 
Order by [Posting Date]'''


# Return all item ledger transactions for a given list of document nos (production orders)
fulltraceQueryfromDocNumber = f'''SELECT  [Posting Date] ,[Entry Type]  ,[Item No_]  ,[Description]  ,[Quantity]  ,
[Unit of Measure Code], [Best Before Date], [Bin Code] , [Document No_], [Document Type], 
[Entry No_], [External Document No_]     ,  [Location Code], [Lot No_],
 [Document Type]
FROM [{database}].[dbo].[{companyname}Item Ledger Entry] where [Document No_] in (%s) 
Order by [Posting Date]'''





    
   



def main():
    
    poNumber = input("Enter your PO Number: ")
    print('Number entered:'+ poNumber)
    # get initial list of lot numbers for PO
    global po
    po = poNumber

    fullorderList = []
    fullLotList = []
    curlots =getInitialLots(poNumber)
    
    for c in curlots:
                fullLotList.append(c)


    anyMoreOrders=True
    loopcounter=0
    '''
    1. Get initial list of lots
    2. Get the production orders that consume them
    3. Get the list out output lots if any
    4. repeat 2 to 3 until there are no more production orders.
    '''

    while(anyMoreOrders):
        loopcounter +=1
        if(loopcounter==10):
            break
      
        print( curlots)
        porders = getConsumingProdOrders(curlots)

        for o in porders:
            fullorderList.append(o)

        print('Prod orders count'+str(len(porders)))
        if(len(porders) ==0):
            anyMoreOrders = False
            break
        else:
            curlots =[]
            templots = getOutPutLots(porders)
          
            curlots = templots
            for c in curlots:
                fullLotList.append(c)

    
    # Dedupe lists..
    fullLotList = list(dict.fromkeys(fullLotList))
    print(fullLotList)
    fullorderList = list(dict.fromkeys(fullorderList))
    print(fullorderList)

    print('Begining full trace')
    fulltraceByLot(fullLotList)
    fulltraceByProdorders(fullorderList)



def getConsumingProdOrders(mylotlist):
    """Get the list of production orders that consume the given lots"""
    prodlisttmp =[]
    prodorderslisttemp =[]
    for l in mylotlist:

        cursor.execute(getProdOrdersQueryBCDB+" '"+l+"'")
        
        mytempporderlist = list(cursor.fetchall())
       
        if(len(mytempporderlist)>0):
            for prodord in mytempporderlist:
                prodorderslisttemp.append(prodord[0])
                
        

   
    
   
    prodlisttmp = list(dict.fromkeys(prodorderslisttemp))
    
    print(prodlisttmp)
    return prodlisttmp

def getOutPutLots (prodorderlist):
    """Get the list of output lot numbers from the given production orders"""
    tmpLotList = []

    for prod in prodorderlist:
        cursor.execute(outputquery+" '"+prod+"'")
        mytempoutlist= list(cursor.fetchall())
        
    for f in mytempoutlist:
         #print(f[0])
         tmpLotList.append(f[0])

    return tmpLotList

def getInitialLots(purorder):
    templist = []
    cursor.execute(purquery,purorder)
    mylotlist = list(cursor.fetchall())
    for p in mylotlist:
        templist.append(p[0])
    return templist

def fulltraceByLot(thelotlistFull):
   
    #turn the %s placeholder in the original query into multiple ? seperated by a comma
    placeholder = '?'
    place_holders = ','.join(placeholder * len(thelotlistFull))
    newquery = fulltraceQueryfromLots % place_holders
    

    sql_query = pd.read_sql_query(newquery,cnxn,params=thelotlistFull) 
    df1 = pd.DataFrame(sql_query)
    df1.to_csv (r'outputtestfrom lots-'+po+'.csv')

def fulltraceByProdorders(theprodorders):
   
    #turn the %s placeholder in the original query into multiple ? seperated by a comma
    placeholder = '?'
    place_holders = ','.join(placeholder * len(theprodorders))
    newquery = fulltraceQueryfromDocNumber % place_holders
    

    sql_query = pd.read_sql_query(newquery,cnxn,params=theprodorders) 
    df1 = pd.DataFrame(sql_query)
    df1.to_csv (r'outputtestfrom prodorders-'+po+'.csv')

    


if __name__ == '__main__':
    main() 
