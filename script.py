import s3fs
import pandas as pd
import geopandas as gpd
import json


import matplotlib.pyplot as plt
import matplotlib.colors as colors

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

# Récupération de la table Dossier Complet Insee
FILE_PATH_S3_DCB = f"{MY_BUCKET}/diffusion/commune-frmetdrom.geojson"
with fs.open(FILE_PATH_S3_DCB, "rb") as file:
    contours_comm = gpd.read_file(file)

#Récupération de la table Mairies (Bucket de Flore)
MY_BUCKET = "floreamice"
FILE_PATH_S3_Mairies = f"{MY_BUCKET}/diffusion/annuaire-de-ladministration-base-de-donnees-locales.csv"
with fs.open(FILE_PATH_S3_Mairies, "rb") as file:
    mairies = pd.read_csv(file,sep=";")


""" DEFINITIONS D'OBJETS """

# Liste des codes insee d'arrondissements
arr_paris = [f"751{str(i).zfill(2)}" for i in range(1, 21)]
arr_marseille = [f"132{str(i).zfill(2)}" for i in range(1, 17)]
arr_lyon = [f"6938{str(i).zfill(1)}" for i in range(1, 10)]

# Dictionnaires
transport_dict = {
    1: "Pas de transport",
    2: "Marche à pied ou rollers",
    3: "Vélo (y compris électrique)",
    4: "Deux-roues motorisé",
    5: "Voiture, camion, fourgonnette",
    6: "Transports en commun"}

cs_labels = {
    1: "Agriculteurs",
    2: "Artisans, commerçants, chef de société",
    3: "Cadres et professions intel.",
    4: "Professions intermédiaires",
    5: "Employés",
    6: "Ouvriers",
    7: "Retraités",
    8: "Autres inactifs"}

""" BASE DE DONNEES MOBPRO """

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

# Labels
df_mobpro_brut['TRANS_LABEL'] = df_mobpro_brut['TRANS'].map(transport_dict)
df_mobpro_brut['CS_LABEL'] = df_mobpro_brut['CS1'].map(cs_labels)


""" BASE DE DONNEES DOSSIER COMPLET """
df_dossier_complet_brut["CODGEO"] = df_dossier_complet_brut["CODGEO"].astype(str).str.zfill(5)


""" BASE DE DONNEE CONTOURS COMMUNES """

# Centroïd des communes
contours_comm["centroid"] = contours_comm.geometry.centroid

# Conversion en projection WGS84 pour avoir les coordonnées en lat/lon
if contours_comm.crs != "EPSG:4326":
    contours_comm = contours_comm.to_crs(epsg=4326)

# Créer un dictionnaire des villes avec leurs coordonnées (latitude, longitude)
coord_villes = dict(zip(
    contours_comm['NOM'].str.lower(),           # Nom des communes en minuscules
    zip(contours_comm.centroid.y, contours_comm.centroid.x)  # Latitude, Longitude
))

# Exemple d'affichage pour les 10 premières villes
for ville, coords in list(coord_villes.items())[:10]:
    print(f"{ville}: {coords}")


""" FONCTIONS D'AFFICHAGE """

def flux(ville_a,ville_b, df_flux):
    '''
    Prend en argument deux codes insee en chaîne de caractère et
    renvoie le nombre de commute (selon le type de flux) entre ces deux villes.
    '''
    flux = df_flux.loc[ville_a, ville_b]
    print(f"Nombre de personnes se déplaçant de {ville_a} vers {ville_b} : {flux}")

def plot_flux_gradient(gdf, couleur, titre, flux_col):
    """
    Affiche une carte avec un gradient de couleur selon le nombre de flux,
    en ne gardant que les communes de la France métropolitaine (DOM-TOM exclus).

    Paramètres :
      - gdf: GeoDataFrame contenant au moins :
           * la colonne de flux (ex: 'flux_depart' ou 'flux_destination'),
           * la colonne 'geometry',
           * et éventuellement 'INSEE_DEP' (code du département).
      - couleur: chaîne ("vert", "jaune" ou "rouge") pour choisir la palette.
      - titre: titre de la carte.
      - flux_col: nom de la colonne contenant le nombre de flux.
      
    La fonction ajoute 1 aux flux pour éviter le log(0) et utilise une normalisation logarithmique.
    """
    # Filtrer pour la France métropolitaine si 'INSEE_DEP' est présente
    if 'INSEE_DEP' in gdf.columns:
        # S'assurer que la colonne est numérique
        gdf['INSEE_DEP'] = pd.to_numeric(gdf['INSEE_DEP'], errors='coerce')
        gdf = gdf[gdf['INSEE_DEP'] < 96]
    
    # Dictionnaire de correspondance entre couleur en français et cmap Matplotlib
    colormap_dict = {
        "vert": "Greens",
        "jaune": "Oranges",
        "rouge": "Reds"
    }
    cmap = colormap_dict.get(couleur.lower(), "Greens")
    
    # Travailler sur une copie pour ne pas modifier l'objet original
    gdf = gdf.copy()
    gdf['flux_log'] = gdf[flux_col] + 1  # Ajouter 1 pour éviter log(0)
    
    # Définir la normalisation logarithmique
    norm = colors.LogNorm(vmin=gdf['flux_log'].min(), vmax=gdf['flux_log'].max())
    
    # Tracer la carte avec légende ajustée
    fig, ax = plt.subplots(1, 1, figsize=(8, 8))
    gdf.plot(column='flux_log',
             cmap=cmap,
             norm=norm,
             linewidth=0,
             ax=ax,
             edgecolor='0.8',
             legend=True,
             legend_kwds={
                 'label': "Flux (log)", 
                 'orientation': "vertical", 
                 'shrink': 0.5,         # Réduit la taille de la barre de couleur à 50%
                 'pad': 0.02,
                 'aspect': 30           # Ajuste la largeur de la légende
             })
    
    # Placer le titre plus en haut avec un pad supérieur
    ax.set_title(titre, fontsize=14, pad=20)
    ax.set_axis_off()
    
    # Ajuster les marges si nécessaire
    plt.subplots_adjust(top=0.95, right=0.85)
    plt.show()


def plot_flux_gradient_zoom(gdf, couleur, titre, flux_col, ville):
    """
    Affiche une carte avec un gradient de couleur selon le nombre de flux,
    zoomée sur une ville spécifiée, avec la ville entourée et son nom affiché.

    Paramètres :
      - gdf: GeoDataFrame des flux.
      - couleur: Couleur pour la palette ('vert', 'jaune', 'rouge').
      - titre: Titre de la carte.
      - flux_col: Colonne contenant le nombre de flux.
      - ville: Nom de la ville pour le zoom.
      - coord_villes: Dictionnaire {ville: (latitude, longitude)}.
      - gdf_villes: GeoDataFrame des communes pour entourer la ville.
    """

    # Vérification si la ville existe
    ville = ville.lower()
    if ville not in coord_villes:
        print(f"⚠️ Erreur : la ville '{ville}' n'est pas trouvée.")
        print(f"Exemples de villes disponibles : {list(coord_villes.keys())[:10]}...")
        return

    # Obtenir les coordonnées de la ville
    lat, lon = coord_villes[ville]

    # Filtrer pour la France métropolitaine
    if 'INSEE_DEP' in gdf.columns:
        gdf['INSEE_DEP'] = pd.to_numeric(gdf['INSEE_DEP'], errors='coerce')
        gdf = gdf[gdf['INSEE_DEP'] < 96]

    # Conversion en projection WGS84
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs(epsg=4326)

    # Palette de couleurs
    cmap = {"vert": "Greens", "jaune": "Oranges", "rouge": "Reds"}.get(couleur.lower(), "Greens")

    # Ajouter une colonne log pour les flux
    gdf['flux_log'] = gdf[flux_col] + 1

    # Échelle logarithmique
    norm = colors.LogNorm(vmin=gdf['flux_log'].min(), vmax=gdf['flux_log'].max())

    # Zoom : 0.35° autour de la ville
    delta = 0.35
    xmin, xmax = lon - delta, lon + delta
    ymin, ymax = lat - delta, lat + delta

    # Identifier la ville cible pour le contour
    ville_cible = gdf[gdf['NOM'].str.lower() == ville]

    # Tracer la carte
    fig, ax = plt.subplots(1, 1, figsize=(10, 10))
    gdf.plot(column='flux_log',
             cmap=cmap,
             norm=norm,
             linewidth=0,
             ax=ax,
             edgecolor='0.8',
             legend=True,
             legend_kwds={'label': "Flux (log)", 'shrink': 0.5, 'pad': 0.02, 'aspect': 30})

    # Dessiner la ville ciblée en contour épais
    if not ville_cible.empty:
        ville_cible.boundary.plot(ax=ax, color='blue', linewidth=2)
        centroid = ville_cible.centroid.iloc[0]
        ax.text(centroid.x, centroid.y, ville.capitalize(), fontsize=10, color='blue', ha='center')

    # Zoom sur la ville
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)

    # Personnalisation
    ax.set_title(f"{titre} - Zoom sur {ville.capitalize()}", fontsize=14, pad=20)
    ax.set_axis_off()

    plt.subplots_adjust(top=0.95, right=0.85)
    plt.show()
