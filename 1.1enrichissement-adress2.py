import pandas as pd
from sqlalchemy import create_engine
import re
from unidecode import unidecode

engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb21")

def EG_Insee_Iris(
    table_entree,
    top_TNP,
    civilite=None,
    prenom=None,
    nom=None,
    complement_nom=None,
    adresse=None,
    complement_adrs=None,
    lieu_dit=None,
    cp=None,
    ville=None,
    id_client=None,
    pays=None,
    email=None,
    tel=None,
):
    refCP_df = pd.read_sql_table("refcp", con=engine)
    ref_IRIS_geo2024_df = pd.read_sql_table("ref_iris_geo2024", con=engine)
    
    def normalize_column_name(col_name):
        if col_name is None:
            return None
        col_name = col_name.lower()
        col_name = re.sub(r"[^a-z0-9]", "_", col_name)
        return col_name

    enriched_df = table_entree.copy()
    if top_TNP == 1:
        split_names = enriched_df[nom].str.split(expand=True)
        enriched_df["gender"] = split_names[0]
        enriched_df["prenom"] = split_names[1]
        enriched_df["nom"] = split_names[2]
    else:
        columns_to_select = [
            col
            for col in [
                civilite,
                prenom,
                nom,
                complement_nom,
                adresse,
                complement_adrs,
                lieu_dit,
                cp,
                ville,
                id_client,
                pays,
                email,
                tel,
            ]
            if col is not None
        ]
        normalized_columns = [normalize_column_name(col) for col in columns_to_select]
        enriched_df = enriched_df[normalized_columns]

    enriched_df.columns = [
        normalize_column_name(col) for col in enriched_df.columns if col is not None
    ]

    enriched_df["ville_normalized"] = enriched_df["ville"].apply(
        lambda x: unidecode(str(x).lower().replace("-", " "))
    )
    refCP_df["nom_de_la_commune_normalized"] = refCP_df["nom_de_la_commune"].apply(
        lambda x: unidecode(str(x).lower().replace("-", " "))
    )

    enriched_df["lieu_dit_normalized"] = enriched_df["lieu_dit"].apply(
        lambda x: unidecode(str(x).lower().replace("-", " ")) if pd.notna(x) else ""
    )
    ref_IRIS_geo2024_df["lib_iris_normalized"] = ref_IRIS_geo2024_df["lib_iris"].apply(
        lambda x: unidecode(str(x).lower().replace("-", " ")) if pd.notna(x) else ""
    )

    enriched_df = enriched_df.merge(
        refCP_df[
            [
                "code_postal",
                "code_commune_insee",
                "nom_de_la_commune",
                "nom_de_la_commune_normalized",
            ]
        ],
        how="left",
        left_on=["cp", "ville_normalized"],
        right_on=["code_postal", "nom_de_la_commune_normalized"],
    )
    enriched_df["c_insee"] = enriched_df["code_commune_insee"]

    enriched_df["c_insee"] = enriched_df["c_insee"].apply(
        lambda x: f"0{x}" if pd.notna(x) and len(str(x)) == 4 else x
    )

    enriched_df = enriched_df.merge(
        ref_IRIS_geo2024_df[["depcom", "lib_iris", "lib_iris_normalized", "code_iris"]],
        how="left",
        left_on=["c_insee", "lieu_dit_normalized"],
        right_on=["depcom", "lib_iris_normalized"],
    )

    enriched_df["c_iris"] = enriched_df["code_iris"].str[-4:].fillna("0000")

    enriched_df["c_qualite_iris"] = enriched_df.apply(
        lambda row: (
            1
            if pd.notna(row["code_iris"]) and pd.notna(row["c_insee"])
            else (2 if pd.notna(row["c_insee"]) else 8)
        ),
        axis=1,
    )

    enriched_df["codgeo"] = enriched_df["c_insee"].fillna("") + enriched_df["c_iris"]

    enriched_df = enriched_df.drop(
        columns=[
            "depcom",
            "lib_iris",
            "code_commune_insee",
            "code_iris",
            "ville_normalized",
            "nom_de_la_commune_normalized",
            "lieu_dit_normalized",
            "lib_iris_normalized",
        ]
    )
    return enriched_df

enriched_table = EG_Insee_Iris(
    table_entree=pd.read_sql_table("true_table_entree", con=engine),
    top_TNP=0,
    cp="cp",
    ville="ville",
    id_client="id_client",
    lieu_dit="lieu_dit",
    civilite="civilite",
    nom="nom",
    prenom="prenom",
)

csv_file = "enriched_clients.csv"
enriched_table.to_csv(csv_file, index=False, encoding="utf-8")
print(f"Fichier '{csv_file}' généré avec succès.")

table_name = "enriched_clients"
enriched_table.to_sql(table_name, con=engine, if_exists='replace', index=False)
print(f"Table '{table_name}' créée et données insérées avec succès dans la base de données 'statsdb21'.")
