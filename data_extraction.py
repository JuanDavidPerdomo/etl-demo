from bson import ObjectId
from datetime import datetime, timedelta, timezone
from conn.conn import external_db, inhouse_datalake_db





def external_source_extraction_data():    
    
    try: 
        external_orders_collection = external_db.orders
        datalake_collection = inhouse_datalake_db.orders

        #array in memory to temporary persist fetched data
        external_orders_fetched = []
        
        utc_timezone = timezone.utc
        current_hour = datetime.now(utc_timezone).replace(minute=0, second=0, microsecond=0)
        previous_hour = current_hour - timedelta(hours=1)

        external_orders_extraction_pipeline = [
        {
            '$match': {
                'shop._id': {
                    '$in': [
                        ObjectId('6331b94faa2af68a4ecad27f'), ObjectId('6331b9b4aa2af68a4ecad351'), ObjectId('6331b982aa2af68a4ecad2ed')
                    ]
                }, 
                'closeDate': {
                    '$gte': previous_hour, 
                    '$lt': current_hour
                }
            }
        }, {
            '$project': {
                'closeDate': 1, 
                'shop._id': 1, 
                'number': 1, 
                'details.price': 1, 
                'details.quantity': 1, 
                'details.quantityReturn': 1, 
                'details.product._id': 1, 
                'details.product.size.value': 1, 
                'details.product.size._id': 1, 
                'details.product.color.name': 1, 
                'details.product.color._id': 1, 
                'details.product.reference.categoryLevel1': 1, 
                'details.product.reference._id': 1, 
                'status': 1
            }
        }, {
            '$unwind': {
                'path': '$details', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$project': {
                'fecha_de_venta': '$closeDate', 
                'tienda': '$shop._id', 
                'pedido': '$number', 
                'precio': '$details.price', 
                'unidades_vendidas': '$details.quantity', 
                'unidades_devueltas': '$details.quantityReturn', 
                'talla': '$details.product.size.value', 
                'color': '$details.product.color.name', 
                'categoria': '$details.product.reference.categoryLevel1', 
                'referencia': '$details.product.reference._id', 
                'status': 1
            }
        }, {
            '$set': {
                'unidades_vendidas': {
                    '$multiply': [
                        '$unidades_vendidas', {
                            '$multiply': [
                                {
                                    '$rand': {}
                                }, 3
                            ]
                        }
                    ]
                }
            }
        }, {
            '$project': {
                'venta_utc': '$fecha_de_venta', 
                'tienda': 1, 
                'pedido': 1, 
                'precio': 1, 
                'unidades_vendidas': {
                    '$round': [
                        '$unidades_vendidas', 0
                    ]
                }, 
                'unidades_devueltas': 1, 
                'talla': 1, 
                'color': 1, 
                'categoria': 1, 
                'referencia': 1, 
                'status': 1, 
                '_id': 0
            }
        }
    ]

        external_orders_fetched_result = external_orders_collection.aggregate(external_orders_extraction_pipeline, cursor = {})

        # extracting data from cursor and staging  on volatile memory
        for external_order in external_orders_fetched_result:
            external_orders_fetched.append(external_order)

        if len(external_orders_fetched) != 0:
            print(external_orders_fetched[0])
            datalake_collection.insert_many(external_orders_fetched)

        return print(f'{len(external_orders_fetched)} documentos extraídos a las {current_hour}')
    
    except Exception as e:
        print(f'se ha presentado el siguiente error {str(e)}')

external_source_extraction_data()