import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from unidecode import unidecode

# Connexion à la base de données
engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb21")

def EG_age_sexe(tb_client, prenom, sexe="NA", age_declare="NA", top_estim_sexe=1, codgeo="codgeo", ajust=0, var_ajust="NA"):
    required_columns = [prenom]
    if sexe != "NA":
        required_columns.append(sexe)
    if age_declare != "NA":
        required_columns.append(age_declare)
    for col in required_columns:
        if col not in tb_client.columns:
            raise ValueError(f"La colonne '{col}' n'existe pas dans tb_client.")
    
    tb_geo = pd.read_sql_table("enriched_clients", con=engine, columns=["id_client", "c_insee", "c_iris", "c_qualite_iris", "codgeo"])
    tbrefgeo = pd.read_sql_table("tbrefgeo", con=engine)
    tb_prenoms = pd.read_sql_table("table_prenoms_table_prenoms", con=engine)


    tb_client["id"] = tb_client["id"].astype(str) 
    tb_geo["id_client"] = tb_geo["id_client"].astype(str)
    
    tb_client = tb_client.merge(tb_geo, left_on=["id"], right_on=["id_client"], how="left")
    
    tb_client["sexe"] = tb_client[sexe] if sexe != "NA" else "NA"

    if top_estim_sexe == 1:
        tb_client["e_sexe"] = np.where(tb_client["sexe"].isin(["H", "F"]), tb_client["sexe"], tb_client[prenom].apply(lambda x: "F" if x[-1].lower() == "a" else "M"))
    else:
        tb_client["e_sexe"] = np.nan

    def estimer_age_geo(client_codgeo):
        geo_data = tbrefgeo[tbrefgeo["codgeo"] == client_codgeo]
        if geo_data.empty:
            return np.nan
        age_estim = (
            (geo_data["age_0_5"] * 2.5 + geo_data["age_6_10"] * 8 + geo_data["age_11_17"] * 14 + geo_data["age_18_24"] * 21 + geo_data["age_25_39"] * 32 + geo_data["age_40_54"] * 47 + geo_data["age_55_64"] * 60 + geo_data["age_65_79"] * 72 + geo_data["age_over_80"] * 85) /
            geo_data[["age_0_5", "age_6_10", "age_11_17", "age_18_24", "age_25_39", "age_40_54", "age_55_64", "age_65_79", "age_over_80"]].sum(axis=1)
        ).values[0]
        return age_estim

    if age_declare != "NA" and age_declare in tb_client.columns:
        tb_client["e_age"] = tb_client[age_declare]
        tb_client["e_top_age_ok"] = 1
    else:
        tb_client["e_age_geo"] = tb_client["codgeo"].apply(estimer_age_geo)
        def estimer_age_prenom_nom(prenom):
            prenom_cleaned = unidecode(prenom).lower()
            prenom_data = tb_prenoms[tb_prenoms["prenom"].str.lower() == prenom_cleaned]
            if prenom_data.empty:
                return np.nan
            current_year = pd.Timestamp.now().year
            ages = []
            for year in range(1913, 2015):
                year_column = f"n{year}"
                if year_column in prenom_data.columns:
                    count = prenom_data[year_column].values[0]
                    if count > 0:
                        ages.append(current_year - year)
            if ages:
                return np.mean(ages)
            return np.nan

        tb_client["e_age_prenom"] = tb_client.apply(lambda x: estimer_age_prenom_nom(x[prenom]), axis=1)
        tb_client["e_age"] = tb_client[["e_age_geo", "e_age_prenom"]].mean(axis=1, skipna=True)
        tb_client["e_top_age_ok"] = np.where(tb_client["e_age"].notna(), 2, 3)

    if ajust == 1:
        if var_ajust != "NA" and var_ajust in tb_client.columns:
            grouped_ages = tb_client.groupby(var_ajust)["e_age"].transform("mean")
            tb_client["e_age"] = tb_client["e_age"] - (tb_client["e_age"] - grouped_ages) / 2
        else:
            mean_age = tb_client["e_age"].mean()
            tb_client["e_age"] = tb_client["e_age"] - (tb_client["e_age"] - mean_age) / 2

    current_year = pd.Timestamp.now().year
    tb_client["e_annee_naissance"] = current_year - tb_client["e_age"].fillna(0).round().astype(int)
    tb_client["e_p_5ans"] = 0.9

    def ajuster_indice_confiance(row):
        age_geo = row["e_age_geo"]
        age_prenom = row["e_age_prenom"]
        if pd.notna(age_geo) and pd.notna(age_prenom):
            if abs(age_geo - age_prenom) < 5:
                return "Confiance ++"
            elif abs(age_geo - age_prenom) < 10:
                return "Confiance +"
            else:
                return "Confiance"
        elif pd.notna(age_geo) or pd.notna(age_prenom):
            return "Confiance -"
        else:
            return "Confiance --"

    tb_client["indice_conf_age"] = tb_client.apply(ajuster_indice_confiance, axis=1)
    tb_client["indice_conf_age"] = np.select(
        [
            tb_client["e_top_age_ok"] == 2,
            tb_client["e_top_age_ok"] == 3,
        ],
        [
            np.where(
                (tb_client["e_age_geo"].notna() & tb_client["e_age_prenom"].notna()) & (abs(tb_client["e_age_geo"] - tb_client["e_age_prenom"]) < 5),
                "Confiance ++",
                np.where((abs(tb_client["e_age_geo"] - tb_client["e_age_prenom"]) < 10), "Confiance +", "Confiance"),
            ),
            "Confiance -",
        ],
        default="Confiance --",
    )

    tb_client = tb_client[
        [
            "e_annee_naissance",
            "e_p_5ans",
            "indice_conf_age",
            "e_top_age_ok",
            "e_age",
            "e_sexe",
            "c_insee",
            "c_iris",
            "c_qualite_iris",
            "civilite",
            "codgeo",
            "complement_adrs",
            "complement_nom",
            "cp",
            "e_age_geo",
            "e_age_prenom",
            "id",
            "pays",
            "prenom",
            "sexe",
            "ville",
        ]
    ]
    return tb_client

resultat = EG_age_sexe(
    tb_client=pd.read_sql_table("true_table_entree", con=engine),
    prenom="prenom",
    sexe="NA",
    age_declare="NA",
    top_estim_sexe=1,
    ajust=0,
    var_ajust="NA",
)

csv_file = "age_sexe_results.csv"
resultat.to_csv(csv_file, index=False, encoding="utf-8")
print(f"Fichier '{csv_file}' généré avec succès.")

table_name = "age_sexe_results"
resultat.to_sql(table_name, con=engine, if_exists='replace', index=False)
print(f"Table '{table_name}' créée et données insérées avec succès dans la base de données 'statsdb21'.")
