import pandas as pd
import xlsxwriter
from sqlalchemy import create_engine

# Connexion à la base de données MySQL
engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb17")

# Charger les données depuis la table enriched_clients
df = pd.read_sql_table("enriched_clients", con=engine)

# Créer un fichier Excel avec XlsxWriter
output_file = 'enriched_clients_with_charts.xlsx'  # Nom du fichier Excel de sortie
writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
workbook = writer.book

def add_sheet_with_excel_chart(sheet_name, columns, graph_data, x_col, y_cols, chart_type='column', is_percentage=False):
    # Ajouter les colonnes sélectionnées dans une feuille
    filtered_df = df[columns]
    filtered_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
    
    # Écrire les données du graphique dans la feuille
    graph_data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=len(filtered_df) + 2, startcol=0)
    
    # Obtenir l'objet de la feuille
    worksheet = writer.sheets[sheet_name]
    
    # Créer un graphique
    chart = workbook.add_chart({'type': chart_type})
    
    # Configurer les séries du graphique (données du graphique) pour chaque colonne
    for i, y_col in enumerate(y_cols):
        chart.add_series({
            'name': y_col,
            'categories': [sheet_name, len(filtered_df) + 3, 0, len(filtered_df) + 3 + len(graph_data) - 1, 0],
            'values': [sheet_name, len(filtered_df) + 3, i + 1, len(filtered_df) + 3 + len(graph_data) - 1, i + 1],
            'fill': {'color': 'blue' if y_col == 'Homme' else 'pink'}
        })
    
    # Configurer les labels et le titre
    chart.set_title({'name': sheet_name + " Graph"})
    chart.set_x_axis({'name': x_col})
    chart.set_y_axis({'name': 'Pourcentage' if is_percentage else 'Counts'})
    
    if is_percentage:
        chart.set_y_axis({'major_gridlines': {'visible': False}, 'min': 0, 'max': 100})
        chart.set_plotarea({'grouping': 'stacked'})  # Empilage pour la répartition en pourcentages
    
    # Insérer le graphique dans la feuille Excel
    worksheet.insert_chart('D2', chart)

# Feuille 1 - Graphique par civilité
sheet_1_data = df['civilite'].value_counts().reset_index(name='Counts')
sheet_1_data.columns = ['civilite', 'Counts']
add_sheet_with_excel_chart(
    'Stats de Sexe',
    columns=['civilite', 'nom', 'prenom'],
    graph_data=sheet_1_data,
    x_col='civilite',
    y_cols=['Counts']
)

# Feuille 2 - Graphique par ville
sheet_2_data = df['ville'].value_counts().reset_index(name='Counts')
sheet_2_data.columns = ['Ville', 'Counts']
add_sheet_with_excel_chart(
    'Stats de Ville',
    columns=['ville', 'nom', 'prenom'],
    graph_data=sheet_2_data,
    x_col='Ville',
    y_cols=['Counts']
)

# Feuille 3 - Pourcentage d'hommes et de femmes
df['sexe'] = df['civilite'].apply(lambda x: 'Homme' if x in ['M', 'Mr', 'Monsieur'] else 'Femme')
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

# Feuille 4 - Nombre d'hommes et de femmes par ville (counts)
gender_city_counts = df.groupby(['ville', 'sexe']).size().unstack().fillna(0).reset_index()
add_sheet_with_excel_chart(
    'Sexe par Ville (Counts)',
    columns=['ville', 'sexe'],
    graph_data=gender_city_counts,
    x_col='ville',
    y_cols=['Homme', 'Femme']
)

# Feuille 5 - Pourcentage d'hommes et de femmes par ville (pourcentages)
gender_city_percentage = df.groupby(['ville', 'sexe']).size().groupby(level=0).apply(lambda x: 100 * x / x.sum()).unstack().fillna(0)

add_sheet_with_excel_chart(
    'Sexe par Ville (Pourcentages)',
    columns=['ville', 'sexe'],
    graph_data=gender_city_percentage,
    x_col='ville',
    y_cols=['Homme', 'Femme'],
    is_percentage=True
)

# Feuille 6 - Graphique par code INSEE
sheet_6_data = df['c_insee'].value_counts().reset_index(name='Counts')
sheet_6_data.columns = ['Code INSEE', 'Counts']
add_sheet_with_excel_chart(
    'Stats par Code INSEE',
    columns=['c_insee'],
    graph_data=sheet_6_data,
    x_col='Code INSEE',
    y_cols=['Counts']
)

# Fermer le fichier Excel
writer.close()
print(f"Fichier Excel '{output_file}' créé avec succès.")
