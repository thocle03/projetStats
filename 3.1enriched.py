import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb18")

enriched_clients = pd.read_sql_table("enriched_clients", con=engine)

maj_reference = pd.read_sql_table("maj_2014_references_maj_2014_references", con=engine)

merged_df = pd.merge(enriched_clients, maj_reference, on='codgeo', how='left')

output_file = 'enriched_clients_with_references.csv'
merged_df.to_csv(output_file, index=False)

new_table_name = 'enriched_clients_with_references'

merged_df.to_sql(new_table_name, con=engine, index=False, if_exists='replace')

print(f"Table '{new_table_name}' créée avec succès dans la base de données.")
