import pandas as pd
import os
import glob
import logging

def get_files(filepath):
    all_transactions_files= []
    for root, dirs, files in os.walk(filepath):
        files= glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_transactions_files.append(os.path.abspath(f))

    return all_transactions_files

##calling get_files() function
transactions= get_files(r"C:\Users\a\AppData\Local\Programs\Python\input_data\starter\transactions")

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


##creating new dataframe, df_transactions_in
df_transactions_in = pd.DataFrame(columns = ['customer_id',
                                             'basket',
                                             'date_of_purchase'])

##inserting transactions data into new dataframe, df_transactions_in
i=0
while True:
    filepath= transactions[i]
    df_transactions_in_2= pd.read_json(filepath, lines=True)
    df_transactions_in= pd.concat([df_transactions_in[['customer_id', 'basket']],
                                   df_transactions_in_2[['customer_id', 'basket']]])

    i += 1
    if i>=91:
        break

##importing products.csv as dataframe, df_products_in
try:
    df_products_in= pd.read_csv(r'C:\Users\a\Desktop\python-assignment-level2-6ed53b4e828af18bc24b1770a3a3e3e70706e785\input_data\starter\products.csv')
except FileNotFoundError:
    print("File not found.")
except Exception:
    # ##logging
    logging.exception("Exception occurred")

##importing customers.csv as dataframe, df_customers_in
try:
    df_customers_in= pd.read_csv(r'C:\Users\a\Desktop\python-assignment-level2-6ed53b4e828af18bc24b1770a3a3e3e70706e785\input_data\starter\customers.csv')
except FileNotFoundError:
    print("File not found.")
except Exception:
    print("Some other exception")


##exploding partitioned column, basket
df_transactions_in = df_transactions_in.set_index('customer_id')['basket'].explode()
df_transactions_in = pd.DataFrame(df_transactions_in.tolist(),
                                  index=df_transactions_in.index).reset_index()

##dropping 'price' column, as it is not required
df_transactions_in= df_transactions_in.drop(["price"],
                                            axis=1)

##removing duplicates & count repeating product_ids corresponds to customer_id
df_transactions_in = df_transactions_in.pivot_table(columns=['customer_id','product_id'],
                                                    aggfunc='size') ##type changed to series

##change to dataframe
df_transactions_in= df_transactions_in.to_frame().reset_index()
##rename column as purchase_count
df_transactions_in.rename( columns={0:'purchase_count'},
                           inplace=True )


##join out_2 with products table
try:
    transactions_with_products= pd.merge(df_transactions_in,
                                     df_products_in,
                                     how= 'left',
                                     on=['product_id'] ).drop(columns=['product_description'])
except pd.errors.MergeError as e:
    print('ok', e)


##join out_3 with customers table
try:
    output= pd.merge(transactions_with_products,
                 df_customers_in,
                 how= 'left',
                 on=['customer_id'] )
except pd.errors.MergeError as e:
    print('ok', e)


##re-ordering the columns
final_output = output[['customer_id',
                       'loyalty_score',
                       'product_id',
                       'product_category',
                       'purchase_count']]

##final output as csv
#final_output.to_csv('final_output.csv')
##final output as json
final_output.to_json('final_output.json')