"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import pandas as pd
import zipfile
from datetime import datetime
def clean_campaign_data():
    input_folder = r"C:\analiticadescriptiva\2024-2-LAB-05-limpieza-de-datos-de-campanas-de-marketing-Dieguini15\files\input"
    output_folder = r"C:\analiticadescriptiva\2024-2-LAB-05-limpieza-de-datos-de-campanas-de-marketing-Dieguini15\files\outputc"

    os.makedirs(output_folder, exist_ok=True)

    client_data = []
    campaign_data = []
    economics_data = []

    for file_name in os.listdir(input_folder):
        if file_name.endswith(".zip"):
            zip_path = os.path.join(input_folder, file_name)
            print(f"Procesando archivo ZIP: {zip_path}")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                for csv_file in zip_ref.namelist():
                    if csv_file.endswith(".csv"):
                        with zip_ref.open(csv_file) as file:
                            try:
                                df = pd.read_csv(file)
                                print(f"Columnas en {csv_file}: {list(df.columns)}")
                                
                                # Procesar client.csv
                                if {"age", "job", "marital", "education", "credit_default", "mortage"}.issubset(df.columns):
                                    client_data.extend(
                                        {
                                            "client_id": row["client_id"],
                                            "age": row["age"],
                                            "job": row["job"].replace(".", "").replace("-", "_"),
                                            "marital": row["marital"],
                                            "education": None if row["education"] == "unknown" else row["education"].replace(".", "_"),
                                            "credit_default": 1 if row["credit_default"] == "yes" else 0,
                                            "mortage": 1 if row["mortage"] == "yes" else 0,
                                        }
                                        for _, row in df.iterrows()
                                    )
                                else:
                                    print(f"Faltan columnas en {csv_file} para procesar client.csv")

                                # Procesar campaign.csv
                                if {"number_contacts", "contact_duration", "previous_campaing_contacts", "previous_outcome", "campaign_outcome", "day", "month"}.issubset(df.columns):
                                    campaign_data.extend(
                                        {
                                            "client_id": row["client_id"],
                                            "number_contacts": row["number_contacts"],
                                            "contact_duration": row["contact_duration"],
                                            "previous_campaing_contacts": row["previous_campaing_contacts"],
                                            "previous_outcome": 1 if row["previous_outcome"] == "success" else 0,
                                            "campaign_outcome": 1 if row["campaign_outcome"] == "yes" else 0,
                                            "last_contact_day": f"2022-{row['month']}-{row['day']:02d}",
                                        }
                                        for _, row in df.iterrows()
                                    )
                                else:
                                    print(f"Faltan columnas en {csv_file} para procesar campaign.csv")

                                # Procesar economics.csv
                                if {"const_price_idx", "eurobor_three_months"}.issubset(df.columns):
                                    economics_data.extend(
                                        {
                                            "client_id": row["client_id"],
                                            "const_price_idx": row["const_price_idx"],
                                            "eurobor_three_months": row["eurobor_three_months"],
                                        }
                                        for _, row in df.iterrows()
                                    )
                                else:
                                    print(f"Faltan columnas en {csv_file} para procesar economics.csv")

                            except pd.errors.EmptyDataError:
                                print(f"Archivo {csv_file} está vacío o tiene errores.")
    
    # Guardar archivos de salida
    if client_data:
        client_df = pd.DataFrame(client_data)
        client_df.to_csv(os.path.join(output_folder, "client.csv"), index=False)
        print(f"Archivo client.csv generado con {len(client_data)} filas.")
    else:
        print("Client data está vacío.")

    if campaign_data:
        campaign_df = pd.DataFrame(campaign_data)
        campaign_df.to_csv(os.path.join(output_folder, "campaign.csv"), index=False)
        print(f"Archivo campaign.csv generado con {len(campaign_data)} filas.")
    else:
        print("Campaign data está vacío.")

    if economics_data:
        economics_df = pd.DataFrame(economics_data)
        economics_df.to_csv(os.path.join(output_folder, "economics.csv"), index=False)
        print(f"Archivo economics.csv generado con {len(economics_data)} filas.")
    else:
        print("Economics data está vacío.")

if __name__ == "__main__":
    clean_campaign_data()
"""
En esta tarea se le pide que limpie los datos de una campaña de
marketing realizada por un banco, la cual tiene como fin la
recolección de datos de clientes para ofrecerls un préstamo.

La información recolectada se encuentra en la carpeta
files/input/ en varios archivos csv.zip comprimidos para ahorrar
espacio en disco.

Usted debe procesar directamente los archivos comprimidos (sin
descomprimirlos). Se desea partir la data en tres archivos csv
(sin comprimir): client.csv, campaign.csv y economics.csv.
Cada archivo debe tener las columnas indicadas.

Los tres archivos generados se almacenarán en la carpeta files/output/.

client.csv:
- client_id
- age
- job: se debe cambiar el "." por "" y el "-" por "_"
- marital
- education: se debe cambiar "." por "_" y "unknown" por pd.NA
- credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
- mortage: convertir a "yes" a 1 y cualquier otro valor a 0

campaign.csv:
- client_id
- number_contacts
- contact_duration
- previous_campaing_contacts
- previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
- campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
- last_contact_day: crear un valor con el formato "YYYY-MM-DD",
    combinando los campos "day" y "month" con el año 2022.

economics.csv:
- client_id
- const_price_idx
- eurobor_three_months



"""



