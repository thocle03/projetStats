import pandas as pd
import xlsxwriter
from sqlalchemy import create_engine
import random

engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb21")
df = pd.read_sql_table("enriched_clients_with_references", con=engine)

output_file = 'enriched_clients_with_charts2.xlsx'
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
        'fill': {'color': random.choice(['blue', 'pink', 'black', 'red', 'green'])}
    })
    chart.set_title({'name': sheet_name + " Graph"})
    chart.set_x_axis({'name': x_col})
    chart.set_y_axis({'name': y_col})
    worksheet.insert_chart('D2', chart)

mean_revenue_by_city = df.groupby('ville')['rev'].mean().reset_index(name='Moyenne Revenu')
add_sheet_with_excel_chart('Moyenne Revenu par Ville', columns=['ville', 'rev'], graph_data=mean_revenue_by_city, x_col='Ville', y_col='Moyenne Revenu')

logement_distribution = df[['propr', 'locat', 'locat_hlm']].sum().reset_index(name='Counts')
logement_distribution.columns = ['Type Logement', 'Counts']
add_sheet_with_excel_chart('Répartition Type Logement', columns=['propr', 'locat', 'locat_hlm'], graph_data=logement_distribution, x_col='Type Logement', y_col='Counts', chart_type='bar')

mean_logement_quality_by_commune = df.groupby('nom_de_la_commune')['c_indice_qualite_logement'].mean().reset_index(name='Qualité Logement Moyenne')
add_sheet_with_excel_chart('Qualité Logement par Commune', columns=['nom_de_la_commune', 'c_indice_qualite_logement'], graph_data=mean_logement_quality_by_commune, x_col='Commune', y_col='Qualité Logement Moyenne')

education_distribution = df[['et_niv0', 'et_niv1', 'et_niv2']].sum().reset_index(name='Counts')
education_distribution.columns = ['Niveau Éducation', 'Counts']
add_sheet_with_excel_chart('Répartition Niveau Éducation', columns=['et_niv0', 'et_niv1', 'et_niv2'], graph_data=education_distribution, x_col='Niveau Éducation', y_col='Counts', chart_type='column')

familles_mono_by_commune = df.groupby('nom_de_la_commune')['tx_fammono'].mean().reset_index(name='Taux Familles Monoparentales')
add_sheet_with_excel_chart('Familles Monoparentales', columns=['nom_de_la_commune', 'tx_fammono'], graph_data=familles_mono_by_commune, x_col='Commune', y_col='Taux Familles Monoparentales')

couple_distribution = df[['tx_coupsenf', 'tx_coupaenf']].sum().reset_index(name='Counts')
couple_distribution.columns = ['Type Couple', 'Counts']
add_sheet_with_excel_chart('Répartition Type Couple', columns=['tx_coupsenf', 'tx_coupaenf'], graph_data=couple_distribution, x_col='Type Couple', y_col='Counts', chart_type='bar')

revenue_quality_by_city = df.groupby('ville')['c_indice_qualite_rev'].mean().reset_index(name='Qualité Revenu Moyenne')
add_sheet_with_excel_chart('Qualité Revenu par Ville', columns=['ville', 'c_indice_qualite_rev'], graph_data=revenue_quality_by_city, x_col='Ville', y_col='Qualité Revenu Moyenne')

writer.close()
print(f"Fichier Excel '{output_file}' créé avec succès.")
