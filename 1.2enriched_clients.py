import pandas as pd
import xlsxwriter
from sqlalchemy import create_engine
import random

engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb18")

df = pd.read_sql_table("enriched_clients", con=engine)

output_file = 'enriched_clients_with_charts.xlsx'  
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
workbook = writer.book

def add_sheet_with_excel_chart(sheet_name, columns, graph_data, x_col, y_cols, chart_type='column', is_percentage=False):
    
    filtered_df = df[columns]
    filtered_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
    
    graph_data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=len(filtered_df) + 2, startcol=0)
    
    worksheet = writer.sheets[sheet_name]
    
    chart = workbook.add_chart({'type': chart_type})
    
    for i, y_col in enumerate(y_cols):
        chart.add_series({
            'name': y_col,
            'categories': [sheet_name, len(filtered_df) + 3, 0, len(filtered_df) + 3 + len(graph_data) - 1, 0],
            'values': [sheet_name, len(filtered_df) + 3, i + 1, len(filtered_df) + 3 + len(graph_data) - 1, i + 1],
            'fill': {'color': 'blue' if y_col in ['Homme','M.'] else 'pink' if y_col in ['Femme', 'Mme'] else random.choice(['black', 'red', 'green'])}
        })
    
    chart.set_title({'name': sheet_name + " Graph"})
    chart.set_x_axis({'name': x_col})
    chart.set_y_axis({'name': 'Pourcentage' if is_percentage else 'Counts'})
    
    if is_percentage:
        chart.set_y_axis({'major_gridlines': {'visible': False}, 'min': 0, 'max': 100})
        chart.set_plotarea({'grouping': 'stacked'})  
    
    worksheet.insert_chart('D2', chart)

sheet_1_data = df['civilite'].value_counts().reset_index(name='Counts')
sheet_1_data.columns = ['civilite', 'Counts']
add_sheet_with_excel_chart(
    'Stats de Sexe',
    columns=['civilite', 'nom', 'prenom'],
    graph_data=sheet_1_data,
    x_col='civilite',
    y_cols=['Counts']
)

sheet_2_data = df['ville'].value_counts().reset_index(name='Counts')
sheet_2_data.columns = ['Ville', 'Counts']
add_sheet_with_excel_chart(
    'Stats de Ville',
    columns=['ville', 'nom', 'prenom'],
    graph_data=sheet_2_data,
    x_col='Ville',
    y_cols=['Counts']
)

df['sexe'] = df['civilite'].apply(lambda x: 'Homme' if x in ['M.', 'Mr', 'Monsieur'] else 'Femme')
gender_percentage = df['sexe'].value_counts(normalize=True).reset_index(name='Pourcentage')
gender_percentage.columns = ['Sexe', 'Pourcentage']
add_sheet_with_excel_chart(
    'Pourcentage Sexe',
    columns=['sexe'],
    graph_data=gender_percentage,
    x_col='Sexe',
    y_cols=['Pourcentage'],
    chart_type='pie'
)

gender_city_counts = df.groupby(['ville', 'sexe']).size().unstack().fillna(0).reset_index()
add_sheet_with_excel_chart(
    'Sexe par Ville (Counts)',
    columns=['ville', 'sexe'],
    graph_data=gender_city_counts,
    x_col='ville',
    y_cols=['Femme', 'Homme']
)

sheet_6_data = df['c_insee'].value_counts().reset_index(name='Counts')
sheet_6_data.columns = ['Code INSEE', 'Counts']
add_sheet_with_excel_chart(
    'Stats par Code INSEE',
    columns=['c_insee'],
    graph_data=sheet_6_data,
    x_col='Code INSEE',
    y_cols=['Counts']
)

insee_percentage = df['c_insee'].value_counts(normalize=True).reset_index(name='Pourcentage')
insee_percentage.columns = ['Code INSEE', 'Pourcentage']
add_sheet_with_excel_chart(
    'Répartition Code INSEE',
    columns=['c_insee'],
    graph_data=insee_percentage,
    x_col='Code INSEE',
    y_cols=['Pourcentage'],
    chart_type='pie'
)

city_percentage = df['ville'].value_counts(normalize=True).reset_index(name='Pourcentage')
city_percentage.columns = ['Ville', 'Pourcentage']
add_sheet_with_excel_chart(
    'Répartition par Ville',
    columns=['ville'],
    graph_data=city_percentage,
    x_col='Ville',
    y_cols=['Pourcentage'],
    chart_type='pie'
)

writer.close()
print(f"Fichier Excel '{output_file}' créé avec succès.")
