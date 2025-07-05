import pandas as pd
import requests
import os
import logging
from dotenv import load_dotenv
from datetime import datetime
import glob
from concat_files import concat

# Configuration du logging pour tester le script dans la console de l'ordinateur
logging.basicConfig(level=logging.INFO)


#Extraction des données depuis openWeather
def extract():
    logging.info("Démarrage de l'étape d'extraction...")

    # la clé est sécurisé dans .env à la source de ce projet
    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    
    if not API_KEY:
        logging.error("Clé API non trouvée. Vérifiez votre fichier .env.")
        return

    villes = ["Antananarivo", "Antsirabe", "Morondava", "Toliara", "Mahajanga"]
    all_weather_data = []

    for ville in villes:
        # Url pour récuperer les informations filtrées par ville
        url = f"https://api.openweathermap.org/data/2.5/weather?q={ville}&units=metric&appid={API_KEY}"
        logging.info(f"Requête envoyée à l'API : {url}")

        response = requests.get(url)

        # S'il y a eu une erreur lors de l'extraction
        if response.status_code != 200:
            logging.error(f"Erreur lors de l'appel API : {response.status_code} - {response.text}")
            continue

        # si l'appel de la clé API a bien été faite 
        data = response.json()

        try:
            weather_data = {
                "Ville": [data.get("name")],
                "Température (°C)": [data["main"].get("temp")],
                "Humidité (%)": [data["main"].get("humidity")],
                "Pression (hPa)": [data["main"].get("pressure")],
                "Vitesse du vent (m/s)": [data["wind"].get("speed")],
                "Date": [pd.to_datetime(data.get("dt"), unit='s')]
            }
        except Exception as e:
            logging.error(f"Erreur lors de l'extraction des données JSON : {e}")
            continue

        df_ville = pd.DataFrame(weather_data)
        all_weather_data.append(df_ville)

    if not all_weather_data:
        logging.error("Aucune donnée météo n'a pu être récupérée.")
        return

    df = pd.concat(all_weather_data, ignore_index=True)
    logging.info("Données extraites avec succès :")
    logging.info(f"\n{df}")
    logging.info(f"Types de données :\n{df.dtypes}")


#Stockage des données extraites en tant que données pas encore nettoyées
    df.to_csv('/home/nonie/exam_IA/untransformed_files/weather.csv', index=False)
    logging.info("Fichier CSV enregistré dans untransformed_files.")



#Nettoyage des données
def transform():
    logging.info("Démarrage de l'étape de nettoyage des données recceuillies...")

    try:
        df = pd.read_csv("/home/nonie/exam_IA/untransformed_files/weather.csv")
    except FileNotFoundError:
        logging.error("Fichier introuvable dans untransformed_files.")
        return

# Normalisation des données , si on veut stocker dans une base de données
    df["Date"] = pd.to_datetime(df["Date"])
    date_str = df.loc[0, "Date"].strftime("%Y-%m-%d")

    df["Ville"] = df["Ville"].str.title()

    logging.info("Données transformées avec succès :")
    logging.info(f"\n{df}")
    logging.info(f"Types de données :\n{df.dtypes}")

    
# Sauvegarde des données nettoyées dans un fichier csv avec sa date d'extraction
    output_path = f'/home/nonie/exam_IA/transformed_files/weather-{date_str}.csv'
    df.to_csv(output_path, index=False)
    logging.info(f"Fichier transformé enregistré : {output_path}")


# Chargement des données prêtes à être exploités
def load():
    logging.info("Démarrage de l'étape de chargement...")

# Recherche de tous les fichiers csv contenant les données nettoyées
    files = glob.glob("/home/nonie/exam_IA/transformed_files/weather-*.csv")
    if not files:
        logging.warning("Aucun fichier trouvé dans transformed_files.")
        return

# fusion des données pour une historique complète
    df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
    df["Date"] = pd.to_datetime(df["Date"], format='mixed', dayfirst=True).dt.date  # faire la précision car il y a eu un soucis de lecture dans looker studio
    df = df.sort_values(by="Date").reset_index(drop=True)


    output_path = "/home/nonie/exam_IA/final_file/weather_history.csv"
    df.to_csv(output_path, index=False)

    logging.info(f"Données finales enregistrées dans : {output_path}")
    logging.info(f"Nombre total de lignes : {len(df)}")


# Pour tester hors Airflow
if __name__ == "__main__":
    extract()
    transform()
    load()
    concat()
