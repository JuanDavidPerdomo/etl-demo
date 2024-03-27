import pandas as pd
import pytz
from datetime import datetime, timezone
from conn.conn import datawarehouse_db
from data_extraction import (
    datalake_orders_extraction_data,
    datalake_categories_extraction_data,
    datalake_references_extraction_data,
    datalake_shops_extraction_data,
)


def tz_transform(dataframe, datetime_collumn_name):

    utc_minus5 = pytz.timezone('America/Bogota')
    dataframe['venta_utc'] = dataframe['venta_utc'].dt.tz_localize('UTC')
    dataframe[datetime_collumn_name] = dataframe['venta_utc'].dt.tz_convert(utc_minus5)

    dataframe[datetime_collumn_name] = pd.to_datetime(dataframe[datetime_collumn_name])
    dataframe = dataframe.set_index(datetime_collumn_name)
    dataframe = dataframe.sort_values(by=datetime_collumn_name)
    dataframe = dataframe.reset_index()
    dataframe['mensual'] = pd.to_datetime(dataframe[datetime_collumn_name].dt.year.apply(str) + '-' + dataframe[datetime_collumn_name].dt.month.apply(str))
    dataframe['diaria'] = pd.to_datetime(dataframe[datetime_collumn_name].dt.year.apply(str) + '-' + dataframe[datetime_collumn_name].dt.month.apply(str) + '-' + dataframe[datetime_collumn_name].dt.day.apply(str))
    dataframe['hora'] = pd.to_datetime(dataframe[datetime_collumn_name]).dt.hour
    dataframe['dia'] = pd.to_datetime(dataframe[datetime_collumn_name]).dt.day_name()

    return dataframe


def data_cleaning_process():
        master_collection = datawarehouse_db.cleanMasterSalesCollection
        utc_timezone = timezone.utc
        datalake_orders_raw_data = datalake_orders_extraction_data()
        print(f'la información obtenida de las orders del datalake es {len(datalake_orders_raw_data)}')
        datalake_references_raw_data = datalake_references_extraction_data()
        datalake_categories_raw_data = datalake_categories_extraction_data()
        datalake_shops_raw_data = datalake_shops_extraction_data()

        
            
        categories_df = pd.DataFrame(datalake_categories_raw_data)
        references_df = pd.DataFrame(datalake_references_raw_data)
        shops_df = pd.DataFrame(datalake_shops_raw_data)
        orders_df = pd.DataFrame(datalake_orders_raw_data)

        lenght_validation = len(orders_df.values.tolist())
        filtered_orders = orders_df.copy()
        filtered_orders = filtered_orders.loc[
            (
                (filtered_orders["status"] == "closed")
                & (filtered_orders["unidades_vendidas"] > 0)
            )
        ]

        for col_name in filtered_orders.columns:
            if type(filtered_orders[col_name][0]) == dict:
                filtered_orders[col_name] = filtered_orders[col_name].apply(
                    lambda x: (
                        x["$oid"] if isinstance(x, dict) and "$oid" in x else x
                    )
                )
            print(f"se limpió la columna: {col_name}\n")

            # cleaning the reference dataframe to standatize to {'_id' : 'name'} format
            references_df = references_df.drop(["categoryLevel1"], axis=1)

            # needs refactory to convert into a function or a class that can allow to addapt more columns
            references_df = references_df.rename(
                columns={"_id": "referencia", "name": "nombre_referencia"}
            )
            categories_df = categories_df.rename(
                columns={"_id": "categoria", "name": "nombre_categoria"}
            )
            shops_df = shops_df.rename(
                columns={"_id": "tienda", "name": "nombre_tienda"}
            )

            index_dict = filtered_orders.index[
                filtered_orders["categoria"].apply(lambda x: isinstance(x, dict))
            ]
            filtered_orders = filtered_orders.drop(index_dict)

            filtered_orders["precio"] = filtered_orders["precio"].astype(int)
            filtered_orders["unidades_vendidas"] = filtered_orders[
                "unidades_vendidas"
            ].astype(int)
            filtered_orders["unidades_devueltas"] = filtered_orders[
                "unidades_devueltas"
            ].astype(int)

            size_mapper = {
                "34": "XS",
                "XL": "XL",
                "Surtida": "Surtida",
                "L": "L",
                "M": "M",
                "36": "L",
                "32": "S",
                "Surtido": "Surtido",
                "Unica": "Unica",
                "XXL": "XXL",
                "38": "XL",
                "S": "S",
                "40": "XXL",
                "30": "XS",
                "L/XL": "L/XL",
                "S/M": "S/M",
            }
            filtered_orders["talla"] = filtered_orders["talla"].map(size_mapper)

            filtered_orders = filtered_orders.merge(
                right=references_df, on="referencia", how="left"
                )
            filtered_orders = filtered_orders.merge(
                right=categories_df, on="categoria", how="left"
            )
            filtered_orders = filtered_orders.merge(
                right=shops_df, on="tienda", how="left"
            )

            filtered_orders = tz_transform(
                dataframe=filtered_orders, datetime_collumn_name="fecha_col"
            )

            cleaned_df = filtered_orders.drop(
                ["tienda", "categoria", "referencia", "_id"], axis=1
            )
            cleaned_df = cleaned_df.loc[~cleaned_df["nombre_referencia"].isna()]

            df_json = cleaned_df.to_dict(orient="records")

            master_collection.insert_many(df_json)

            return print(
                f"{len(df_json)} documentos insertados a las {datetime.now(utc_timezone)}"
            )







def cronjob_log():
    try:
        utc_timezone = pytz.timezone('America/Bogota')
        col_hour = datetime.now(utc_timezone)
        message = "im still working"
        cronjob_collection = datawarehouse_db.cronjobsHealthyLogs
        cronjob_collection.insert_one({}, {'cron_report': col_hour, 'message': message})
    except Exception as e:
        print(f"se ha presentado el siguiente error {str(e)}")
    return print(f'ejecución realizada a las : {col_hour} en UTC-5')
