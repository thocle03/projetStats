import pandas as pd
import xlsxwriter

# Charger le fichier CSV
csv_file = './sorties_elfy.csv'  # Remplacez par le chemin de votre fichier CSV
df = pd.read_csv(csv_file, sep=';', low_memory=False)

# Créer un fichier Excel avec XlsxWriter
output_file = 'output_with_charts.xlsx'  # Nom du fichier Excel de sortie
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
workbook = writer.book

# Fonction pour ajouter une feuille avec des colonnes sélectionnées et un graphique Excel natif
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
        'values':     [sheet_name, len(filtered_df) + 3, 1, len(filtered_df) + 3 + len(graph_data) - 1, 1],
    })

    # Configurer les labels et le titre
    chart.set_title({'name': sheet_name + " Graph"})
    chart.set_x_axis({'name': x_col})
    chart.set_y_axis({'name': y_col})

    # Insérer le graphique dans la feuille Excel
    worksheet.insert_chart('D2', chart)

# Feuille 1 - Graphique par sexe
sheet_1_data = df.groupby('Sex').size().reset_index(name='Counts')
add_sheet_with_excel_chart(
    'Sheet 1',
    columns=['Nom', 'Prenom', 'Sex'],  # Choisissez les colonnes que vous voulez
    graph_data=sheet_1_data,
    x_col='Sex',     # Colonne pour l'axe X
    y_col='Counts'   # Colonne pour l'axe Y
)

# Feuille 2 - Graphique moyen de RisqueV4 par Ville
sheet_2_data = df.groupby('Ville').agg({'RisqueV4': 'mean'}).reset_index()
add_sheet_with_excel_chart(
    'Sheet 2',
    columns=['Ville', 'RisqueV4', 'PoidsV4'],  # Autres colonnes
    graph_data=sheet_2_data,
    x_col='Ville',   # Colonne pour l'axe X
    y_col='RisqueV4' # Colonne pour l'axe Y
)

# Fermer le fichier Excel
writer.close()

print(f"Fichier Excel {output_file} créé avec succès.")