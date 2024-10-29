import pandas as pd
from sqlalchemy import create_engine

# Connexion à la base de données
engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb15")

def EG_Enrichissement_Geomk(table_initiale, codgeo, var_sexe, var_age):
    # Charger les tables de référence
    tb_geo = pd.read_sql_table("tbrefgeo", con=engine)
    
    # Merge avec les données géographiques
    enriched_df = table_initiale.merge(tb_geo, how="left", left_on=codgeo, right_on="codgeo")
    
    # Ajout de données socio-démographiques
    enriched_df["e_PCS"] = "Cadre"  # Catégorie socioprofessionnelle (exemple)
    enriched_df["c_indice_qualite_pcs"] = 1  # Qualité de l'estimation PCS
    enriched_df["statut_habitation"] = "Individuelle"  # Statut d'habitation (exemple)
    enriched_df["nb_enfants"] = 2  # Estimation du nombre d'enfants (exemple)
    enriched_df["typologie_commune"] = "Urbaine"  # Typologie de la commune (exemple)
    enriched_df["niveau_revenu"] = "Élevé"  # Niveau de revenu (exemple)
    enriched_df["decile_revenu"] = 9  # Décile de revenu (exemple)
    enriched_df["region"] = "Île-de-France"  # Région (exemple)

    return enriched_df

# Exemple d'utilisation
table_initiale = pd.read_sql_table("true_table_entree", con=engine)
enriched_data = EG_Enrichissement_Geomk(
    table_initiale=table_initiale,
    codgeo="codgeo",
    var_sexe="sexe",
    var_age="age",
)

# Enregistrer les résultats dans un fichier CSV
csv_file = "geomarketing_enriched_data.csv"
enriched_data.to_csv(csv_file, index=False, encoding="utf-8")
print(f"Fichier '{csv_file}' généré avec succès.")

# Créer une nouvelle table et insérer les données dans la base de données
table_name = "geomarketing_enriched_data"
enriched_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
print(f"Table '{table_name}' créée et données insérées avec succès dans la base de données 'statsdb15'.")
