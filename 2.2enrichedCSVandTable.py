import pandas as pd
import xlsxwriter
from sqlalchemy import create_engine
import random

engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb21")
df = pd.read_sql_table("age_sexe_results", con=engine)
output_file = 'age_sexe_with_charts.xlsx'
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
workbook = writer.book

def add_sheet_with_excel_chart(sheet_name, columns, graph_data, x_col, y_col, chart_type='column'):
    filtered_df = df[columns]
    filtered_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
    graph_data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=len(filtered_df) + 2, startcol=0)
    worksheet = writer.sheets[sheet_name]
    chart = workbook.add_chart({'type': chart_type})
    chart.add_series({
        'name': y_col,
        'categories': [sheet_name, len(filtered_df) + 3, 0, len(filtered_df) + 3 + len(graph_data) - 1, 0],
        'values': [sheet_name, len(filtered_df) + 3, 1, len(filtered_df) + 3 + len(graph_data) - 1, 1],
        'fill': {'color': 'blue' if y_col == 'Homme' else 'pink' if y_col in ['Femme', 'Mme'] else random.choice(['black', 'red', 'green'])}
    })
    chart.set_title({'name': sheet_name + " Graph"})
    chart.set_x_axis({'name': x_col})
    chart.set_y_axis({'name': y_col})
    worksheet.insert_chart('D2', chart)

# 1. Distribution de l'âge
df['age'] = 2023 - df['e_annee_naissance']
age_distribution = df['age'].value_counts().sort_index().reset_index(name='Counts')
age_distribution.columns = ['Age', 'Counts']
add_sheet_with_excel_chart('Distribution Âge', columns=['age'], graph_data=age_distribution, x_col='Âge', y_col='Counts')

# 2. Moyenne d'âge par ville
mean_age_by_city = df.groupby('ville')['age'].mean().reset_index(name='Moyenne Âge')
add_sheet_with_excel_chart('Moyenne Âge par Ville', columns=['ville', 'age'], graph_data=mean_age_by_city, x_col='Ville', y_col='Moyenne Âge')

# 3. Répartition par tranche d'âge
df['tranche_age'] = pd.cut(df['age'], bins=[0, 20, 40, 60, 80, 100], labels=['<20', '20-40', '40-60', '60-80', '80+'])
age_range_distribution = df['tranche_age'].value_counts().reset_index(name='Counts')
age_range_distribution.columns = ['Tranche Âge', 'Counts']
add_sheet_with_excel_chart('Répartition Tranche Âge', columns=['tranche_age'], graph_data=age_range_distribution, x_col='Tranche Âge', y_col='Counts', chart_type='bar')

# 4. Âge moyen par sexe
mean_age_by_gender = df.groupby('sexe')['age'].mean().reset_index(name='Âge Moyen')
add_sheet_with_excel_chart('Âge Moyen par Sexe', columns=['sexe', 'age'], graph_data=mean_age_by_gender, x_col='Sexe', y_col='Âge Moyen')

writer.close()
print(f"Fichier Excel '{output_file}' créé avec succès.")
