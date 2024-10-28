import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root@localhost:3306/statsdb11")


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

    # Collect column names that are not None
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

    # Copy and filter the DataFrame using selected columns
    enriched_df = table_entree.copy()[columns_to_select]

    enriched_df = enriched_df.merge(
        refCP_df[["code_postal", "code_commune_insee"]],
        how="left",
        left_on=[cp],
        right_on=["code_postal"],
    )
    enriched_df["c_insee"] = enriched_df["code_commune_insee"]

    enriched_df = enriched_df.merge(
        ref_IRIS_geo2024_df[["depcom", "lib_iris", "code_iris"]],
        how="left",
        left_on=["c_insee", lieu_dit],
        right_on=["depcom", "lib_iris"],
    )

    enriched_df["c_iris"] = enriched_df["code_iris"].str[-4:]

    enriched_df["c_qualite_iris"] = enriched_df.apply(
        lambda row: 1 if pd.notna(row["c_iris"]) else 8, axis=1
    )
    enriched_df["codgeo"] = enriched_df["c_insee"].fillna("") + enriched_df[
        "c_iris"
    ].fillna("")

    enriched_df = enriched_df.drop(
        columns=["depcom", "lib_iris", "code_commune_insee", "code_iris"]
    )
    return enriched_df


db_connection_string = "mysql+pymysql://root@localhost:3306/statsdb11"

enriched_table = EG_Insee_Iris(
    table_entree=pd.read_sql_table("clients", con=engine),
    top_TNP=1,
    cp="cp",
    ville="ville",
    id_client="id_client",
    lieu_dit="lieu_dit",
)

print(enriched_table)