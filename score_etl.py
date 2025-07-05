import pandas as pd
import os
import logging


logging.basicConfig(level=logging.INFO)



# Chargement de l'historique 
def extract_weather():
    logging.info("Chargement des données depuis : /home/nonie/exam_IA/final_file/weather_history.csv")
    df = pd.read_csv("/home/nonie/exam_IA/final_file/weather_history.csv")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Mois"] = df["Date"].dt.month
    df["Année"] = df["Date"].dt.year

    def convertir_mois_en_nom(mois_num):
        noms_mois = {
            1: "janvier", 2: "février", 3: "mars", 4: "avril",
            5: "mai", 6: "juin", 7: "juillet", 8: "août",
            9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"
        }
        return noms_mois.get(mois_num, "inconnu")

    df["Nom du mois"] = df["Mois"].apply(convertir_mois_en_nom)
    logging.info("Données enrichies avec les colonnes 'Mois', 'Année' et 'Nom du mois'")

    df.to_csv("/home/nonie/exam_IA/temp/intermediate.csv", index=False)
    logging.info("Données sauvegardées temporairement dans : /home/nonie/exam_IA/temp/intermediate.csv")

# Transformation
def transform_weather():
    logging.info("Chargement des données pour transformation depuis : /home/nonie/exam_IA/temp/intermediate.csv")
    df = pd.read_csv("/home/nonie/exam_IA/temp/intermediate.csv")

    def score_row(row):
        score = 0
        if 22 <= row["Température (°C)"] <= 28:
            score += 2
        elif 20 <= row["Température (°C)"] <= 30:
            score += 1

        if row["Vitesse du vent (m/s)"] <= 3:
            score += 2
        elif row["Vitesse du vent (m/s)"] <= 5:
            score += 1

        if 40 <= row["Humidité (%)"] <= 70:
            score += 2
        elif 30 <= row["Humidité (%)"] <= 80:
            score += 1

        return score

    df["Score météo"] = df.apply(score_row, axis=1)
    logging.info("Scores météo calculés et ajoutés à la DataFrame")

    df.to_csv("/home/nonie/exam_IA/temp/intermediate.csv", index=False)
    logging.info("Données transformées sauvegardées dans : /home/nonie/exam_IA/temp/intermediate.csv")


# stockage des résultats
def load_weather():
    logging.info("Chargement des données finales depuis : /home/nonie/exam_IA/temp/intermediate.csv")
    df = pd.read_csv("/home/nonie/exam_IA/temp/intermediate.csv")

    mean_score = df.groupby(["Ville", "Mois", "Nom du mois"])["Score météo"].mean().reset_index()
    mean_score.to_csv("/home/nonie/exam_IA/final_file/weather_score.csv", index=False)
    logging.info("Scores moyens sauvegardés dans : /home/nonie/exam_IA/final_file/weather_score.csv")

    best_months = mean_score.sort_values(["Ville", "Score météo"], ascending=[True, False]).groupby("Ville").head(3)
    best_months.to_csv("/home/nonie/exam_IA/final_file/best_score.csv", index=False)
    logging.info("Top 3 des meilleurs mois par ville sauvegardés dans : /home/nonie/exam_IA/final_file/best_score.csv")

# Pour test hors Airflow
if __name__ == "__main__":
    extract_weather()
    transform_weather()
    load_weather()
