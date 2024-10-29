import pandas as pd
import xlsxwriter
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb17")

df = pd.read_sql_table("enriched_clients", con=engine)

output_file = 'enriched_clients_with_charts.xlsx'
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
workbook = writer.book

def add_sheet_with_excel_chart(sheet_name, columns, graph_data, x_col, y_col):
    filtered_df = df[columns]
    filtered_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
    
    graph_data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=len(filtered_df) + 2, startcol=0)
    
    worksheet = writer.sheets[sheet_name]
    
    chart = workbook.add_chart({'type': 'column'})
    
    chart.add_series({
        'name': y_col,
        'categories': [sheet_name, len(filtered_df) + 3, 0, len(filtered_df) + 3 + len(graph_data) - 1, 0],
        'values': [sheet_name, len(filtered_df) + 3, 1, len(filtered_df) + 3 + len(graph_data) - 1, 1],
    })
    
    # Configurer les labels et le titre
    chart.set_title({'name': sheet_name + " Graph"})
    chart.set_x_axis({'name': x_col})
    chart.set_y_axis({'name': y_col})
    
    worksheet.insert_chart('D2', chart)

sheet_1_data = df.groupby('civilite').size().reset_index(name='Counts')
add_sheet_with_excel_chart(
    'Stats de Sexe',
    columns=['civilite', 'nom', 'prenom'],
    graph_data=sheet_1_data,
    x_col='civilite',
    y_col='Counts'
)

sheet_2_data = df['ville'].value_counts().reset_index(name='Counts')
sheet_2_data.columns = ['Ville', 'Counts']
add_sheet_with_excel_chart(
    'Stats de Ville',
    columns=['ville', 'nom', 'prenom'],
    graph_data=sheet_2_data,
    x_col='Ville',
    y_col='Counts'
)

writer.close()
print(f"Fichier Excel '{output_file}' créé avec succès.")
