import pandas as pd
import xlsxwriter
from sqlalchemy import create_engine

# Connexion à la base de données MySQL
engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb17")

# Charger les données depuis la table age_sexe_results
df = pd.read_sql_table("age_sexe_results", con=engine)

# Créer un fichier Excel avec XlsxWriter
output_file = 'age_sexe_with_charts.xlsx'  # Nom du fichier Excel de sortie
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
workbook = writer.book

def add_sheet_with_excel_chart(sheet_name, columns, graph_data, x_col, y_col):
    # Ajouter les colonnes sélectionnées dans une feuille
    filtered_df = df[columns]
    filtered_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
    
    # Écrire les données du graphique dans la feuille
    graph_data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=len(filtered_df) + 2, startcol=0)
    
    # Obtenir l'objet de la feuille
    worksheet = writer.sheets[sheet_name]
    
    # Créer un graphique à barres
    chart = workbook.add_chart({'type': 'column'})
    
    # Configurer les séries du graphique (données du graphique)
    chart.add_series({
        'name': y_col,
        'categories': [sheet_name, len(filtered_df) + 3, 0, len(filtered_df) + 3 + len(graph_data) - 1, 0],
        'values': [sheet_name, len(filtered_df) + 3, 1, len(filtered_df) + 3 + len(graph_data) - 1, 1],
    })
    
    # Configurer les labels et le titre
    chart.set_title({'name': sheet_name + " Graph"})
    chart.set_x_axis({'name': x_col})
    chart.set_y_axis({'name': y_col})
    
    # Insérer le graphique dans la feuille Excel
    worksheet.insert_chart('D2', chart)

# Feuille 1 - Graphique de l'année de naissance en fonction du prénom
sheet_1_data = df.groupby('prenom')['e_annee_naissance'].mean().reset_index(name='Année Moyenne de Naissance')
add_sheet_with_excel_chart(
    'Année de Naissance',
    columns=['prenom', 'e_annee_naissance'],
    graph_data=sheet_1_data,
    x_col='prenom',
    y_col='Année Moyenne de Naissance'
)

# Fermer le fichier Excel
writer.close()
print(f"Fichier Excel '{output_file}' créé avec succès.")
