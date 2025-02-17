import s3fs
import pandas as pd
import geopandas as gpd
import json


""" IMPORT BASES DE DONNEES """

# Information d'accès au cloud MinIO (Bucket de William)
fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": "https://minio.lab.sspcloud.fr"})
MY_BUCKET = "williamolivier"

# Récupération de la table Dossier Complet Insee
FILE_PATH_S3_DCB = f"{MY_BUCKET}/diffusion/df_dossier_complet_brut.csv"
with fs.open(FILE_PATH_S3_DCB, "rb") as file:
    df_dossier_complet_brut = pd.read_csv(file)

# Récupération de la table Métadonnées Dossier Complet Insee
FILE_PATH_S3_MDCB = f"{MY_BUCKET}/diffusion/df_meta_dossier_complet_brut.csv"
with fs.open(FILE_PATH_S3_MDCB, "rb") as file:
    df_meta_dossier_complet_brut = pd.read_csv(file)

# Récupération de la table Métadonnées MOBPRO
FILE_PATH_S3_MMB = f"{MY_BUCKET}/diffusion/df_meta_mobpro_brut.csv"
with fs.open(FILE_PATH_S3_MMB, "rb") as file:
    df_meta_mobpro_brut = pd.read_csv(file)

# Récupération de la table MOBPRO
FILE_PATH_S3_MB = f"{MY_BUCKET}/diffusion/df_mobpro_brut.csv"
with fs.open(FILE_PATH_S3_MB, "rb") as file:
    df_mobpro_brut = pd.read_csv(file)

# Récupération de la table Contours des communes
FILE_PATH_S3_DCB = f"{MY_BUCKET}/diffusion/a-com2022-topo-2154.json"
with fs.open(FILE_PATH_S3_DCB, "rb") as file:
    comm = gpd.read_file(file)


""" DEFINITIONS D'OBJETS """

# Liste des codes insee d'arrondissements
arr_paris = [f"751{str(i).zfill(2)}" for i in range(1, 21)]
arr_marseille = [f"132{str(i).zfill(2)}" for i in range(1, 17)]
arr_lyon = [f"6938{str(i).zfill(1)}" for i in range(1, 10)]

# Dictionnaire des types de transports
transport_dict = {
    1: "Pas de transport",
    2: "Marche à pied (ou rollers, patinette)",
    3: "Vélo (y compris à assistance électrique)",
    4: "Deux-roues motorisé",
    5: "Voiture, camion, fourgonnette",
    6: "Transports en commun"
}

""" BASE DE DONNEE MOBPRO """

# Homogénéisation des codes insee (tous en chaîne de caractère)
df_mobpro_brut["COMMUNE"] = df_mobpro_brut["COMMUNE"].astype(str).str.zfill(5)
df_mobpro_brut["DCLT"] = df_mobpro_brut["DCLT"].astype(str).str.zfill(5)
df_mobpro_brut["ARM"] = df_mobpro_brut["ARM"].astype(str).str.zfill(5)

# Conversion des colonnes en numérique
df_mobpro_brut['NPERR'] = pd.to_numeric(df_mobpro_brut['NPERR'], errors='coerce')
df_mobpro_brut['INPSM'] = pd.to_numeric(df_mobpro_brut['INPSM'], errors='coerce')
df_mobpro_brut['INPOM'] = pd.to_numeric(df_mobpro_brut['INPOM'], errors='coerce')
df_mobpro_brut['INEEM'] = pd.to_numeric(df_mobpro_brut['INEEM'], errors='coerce')

# COMMUNE mise à l'échelle de l'arrondissement
df_mobpro_brut.loc[df_mobpro_brut['COMMUNE'].isin(['75056', '13055', '69123']), 'COMMUNE'] = df_mobpro_brut['ARM']