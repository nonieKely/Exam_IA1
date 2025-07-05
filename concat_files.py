import pandas as pd
import logging
import glob
import os

logging.basicConfig(level=logging.INFO)

# Dictionnaire des villes
COORDS_TO_CITY = {
    (-18.945518, 47.527092): "Antananarivo",
    (-18.875, 47.5): "Antsirabe",
    (-15.75, 46.375): "Mahajanga",
    (-19.75, 46.875): "Toliara",
    (-20.25, 44.375): "Morondava"
}

def detect_city(lat, lon):
    for (c_lat, c_lon), city in COORDS_TO_CITY.items():
        if round(lat, 2) == round(c_lat, 2) and round(lon, 2) == round(c_lon, 2):
            return city
    return "Inconnue"

def concat():
    logging.info("Fusion des fichiers historiques...")

    historical_files = glob.glob("/home/nonie/exam_IA/historical_weather/*.csv")
    df_historical_list = []

    for file in historical_files:
        try:
            with open(file, 'r') as f:
                lines = f.readlines()
                coord_line = lines[1].split(',')  # 2ème ligne = index 1
                lat = float(coord_line[0].strip())
                lon = float(coord_line[1].strip())

            df = pd.read_csv(file, skiprows=2)
            ville = detect_city(lat, lon)
            df["Ville"] = ville

            df = df.rename(columns={
                'time': 'Date',
                'temperature_2m_mean (°C)': 'Température (°C)',
                'relative_humidity_2m_mean (%)': 'Humidité (%)',
                'surface_pressure_mean (hPa)': 'Pression (hPa)',
                'wind_speed_10m_max (m/s)': 'Vitesse du vent (m/s)'
            })

            df_historical_list.append(df)

        except Exception as e:
            logging.error(f"Erreur dans {file} : {e}")
            continue

    if not df_historical_list:
        logging.warning("Aucune donnée historique chargée.")
        df_historical = pd.DataFrame(columns=["Ville", "Température (°C)", "Humidité (%)", "Pression (hPa)", "Vitesse du vent (m/s)", "Date"])
    else:
        df_historical = pd.concat(df_historical_list, ignore_index=True)
        # Supprimer les lignes où la ville est "Inconnue"
        df_historical = df_historical[df_historical["Ville"] != "Inconnue"]

    # Lecture du fichier weather_history.csv existant
    logging.info("Lecture du fichier final existant s'il existe déjà...")

    existing_path = "/home/nonie/exam_IA/final_file/weather_history.csv"
    if os.path.exists(existing_path):
        try:
            df_existing = pd.read_csv(existing_path)
        except Exception as e:
            logging.warning(f"Erreur lors de la lecture de l'ancien fichier weather_history.csv : {e}")
            df_existing = pd.DataFrame(columns=["Ville", "Température (°C)", "Humidité (%)", "Pression (hPa)", "Vitesse du vent (m/s)", "Date"])
    else:
        df_existing = pd.DataFrame(columns=["Ville", "Température (°C)", "Humidité (%)", "Pression (hPa)", "Vitesse du vent (m/s)", "Date"])

    # Fusion finale
    logging.info("Fusion finale des données existantes + historiques...")

    final_df = pd.concat([df_existing, df_historical], ignore_index=True)

    final_columns = ["Ville", "Température (°C)", "Humidité (%)", "Pression (hPa)", "Vitesse du vent (m/s)", "Date"]
    final_df = final_df[final_columns]

    # Nettoyage des dates et suppression des doublons
    final_df["Date"] = pd.to_datetime(final_df["Date"], errors="coerce").dt.date
    final_df = final_df.dropna(subset=["Date"])
    final_df = final_df.drop_duplicates(subset=["Ville", "Date"], keep="last")
    final_df = final_df.sort_values(by="Date").reset_index(drop=True)

    # Sauvegarde finale
    output_path = "/home/nonie/exam_IA/final_file/weather_history.csv"
    final_df.to_csv(output_path, index=False)

    logging.info(f"Données finales enregistrées dans : {output_path}")
    logging.info(f"Nombre total de lignes : {len(final_df)}")

if __name__ == "__main__":
    concat()
