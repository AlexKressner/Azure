import logging
import pyodbc

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    sachnummer = req.params.get('sachnummer')
    lieferant = req.params.get('lieferant')
    abladestelle = req.params.get('abladestelle')

    if not req.params:
        logging.info(f'Incomplete supply chain: {sachnummer}-{lieferant}-{abladestelle}')
        return func.HttpResponse(
             f"Query string is incomplete. You gave: {sachnummer}-{lieferant}-{abladestelle}",
             status_code=400
        )
    
    #Connect to SQL-Server
    connection_string = """ Driver={ODBC Driver 17 for SQL Server};
                            Server=tcp:logistics-db.database.windows.net,1433;
                            Database=Logistics;
                            Uid=admin_log;
                            Pwd=<>;
                            Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"""
    cnxn = pyodbc.connect(connection_string)
    logging.info(cnxn)

    #Fetch stock data of supply chain
    cursor = cnxn.cursor()
    cursor.execute(""" Select Erwartungswert_Nachfrage 
                        From dispoparameter 
                        Where Sachnummer = ? 
                            And Lieferantennummer = ?
                            And Abladestelle = ?""", sachnummer,lieferant,abladestelle
                    )
    row = cursor.fetchone()        

    #Return database entry
    if not row:
        return func.HttpResponse(
            f"Keine Daten für folgende Supply Chain gefunden: {sachnummer}-{lieferant}-{abladestelle}!",
            status_code=200
            )
    return func.HttpResponse(
            f"Erwartungswert der Nachfrage für die Supply Chain {sachnummer}-{lieferant}-{abladestelle} ist {row.Erwartungswert_Nachfrage}!",
            status_code=200
            )