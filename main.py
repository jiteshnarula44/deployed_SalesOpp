from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import io
from azure.storage.blob import BlobServiceClient, ContentSettings
from datetime import datetime

app = FastAPI()

# SQL Server connection string configuration
def get_sql_connection_string():
    server = "jitesh-sql-server.database.windows.net"
    database = "kano.backup"
    username = "training_db_kano"
    password = "234vb&Qx5#"
    connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"
    return connection_string

# Azure Blob Storage configuration
def get_blob_service_client():
    connection_string = "DefaultEndpointsProtocol=https;AccountName=narula12storage;AccountKey=s8rUHL11ngvXxzJMatsIPT1UKaQsXMw61lKTTb7xA4bM2AawsFIpuf0I4Ty5rwsPpqg4t6IDGe6c+AStCavGIg==;EndpointSuffix=core.windows.net"
    return BlobServiceClient.from_connection_string(connection_string)

@app.get("/sales_rep")
def get_sales_rep_data():
    try:
        engine = create_engine(get_sql_connection_string())
        blob_service_client = get_blob_service_client()
        container_name = "intermediate"

        # Load sales rep names from Blob Storage
        blob_client = blob_service_client.get_blob_client(container=container_name, blob="External Rep Agency Distribution Emails.csv")
        data = blob_client.download_blob().readall()
        sales_rep_df = pd.read_csv(io.BytesIO(data))

        # Get current month and year
        now = datetime.now()
        current_month = now.strftime("%B")  # Full month name (e.g., "November")
        current_year = now.strftime("%Y")    # Year (e.g., "2024")

        for sales_rep in sales_rep_df['Company /External Sales Rep']:
            query = """
    SELECT 
        T.Sales_Rep,
        T.Customer_Category,
        T.Customer_Key, 
        T.Top_Level_Parent, 
        T.Customer_Name,
        T.Price_Level, 
        T.Industry_Type, 
        T.Shipping_State, 
        T.Shipping_City,
        T.Total_Net_Amount,
        T.TimeRange
    FROM (
        SELECT 
            DC.Sales_Rep,
            CC.Customer_Category,
            DC.Customer_Key, 
            DC.Top_Level_Parent, 
            DC.Customer_Name,
            DC.Price_Level, 
            I.Industry_Type, 
            F.Shipping_State, 
            F.Shipping_City,
            SUM(F.Net_Amount) AS Total_Net_Amount,
            CASE
                WHEN MAX(F.Date_Created_Date) BETWEEN DATEADD(MONTH, -6, GETDATE()) AND DATEADD(MONTH, -3, GETDATE()) THEN '3 to 6 months'
                WHEN MAX(F.Date_Created_Date) BETWEEN DATEADD(MONTH, -9, GETDATE()) AND DATEADD(MONTH, -6, GETDATE()) THEN '6 to 9 months'
                WHEN MAX(F.Date_Created_Date) BETWEEN DATEADD(MONTH, -12, GETDATE()) AND DATEADD(MONTH, -9, GETDATE()) THEN '9 to 12 months'
                WHEN MAX(F.Date_Created_Date) < DATEADD(MONTH, -12, GETDATE()) THEN 'Greater than 12 months'
            END AS TimeRange
        FROM 
            [dwh].[Dim_Customer] DC
        INNER JOIN 
            [dwh].[Dim_Customer_Category] CC ON DC.Customer_Category_Key = CC.Cust_Category_ID
        INNER JOIN 
            [dwh].[Dim_Industry] I ON I.Industry_Type_ID = DC.Industry_Type_key
        INNER JOIN 
            [dwh].[Fact_Transaction_Line] F ON F.Customer_Key = DC.Customer_Key
        WHERE 
            DC.Sales_Rep = %s
            AND DC.Is_Active = 1
            AND CC.Is_Active = 1
            AND I.Is_Active = 1
        GROUP BY 
            DC.Sales_Rep, 
            CC.Customer_Category,
            DC.Customer_Key, 
            DC.Top_Level_Parent, 
            DC.Customer_Name, 
            DC.Price_Level, 
            I.Industry_Type,
            F.Shipping_State, 
            F.Shipping_City
    ) T
    WHERE 
        T.Sales_Rep IS NOT NULL 
        AND T.TimeRange IS NOT NULL
    ORDER BY  
        T.Shipping_State ASC, 
        T.Shipping_City ASC
"""
            # Pass sales_rep as a parameter
            df = pd.read_sql_query(query, engine, params=(sales_rep,))

            # Check if DataFrame is empty before processing
            if df.empty:
                print(f"No data found for {sales_rep}. Skipping...")
                continue

            # Process and save as Excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                for time_range, group_df in df.groupby("TimeRange"):
                    sheet_df = group_df.drop(columns=["TimeRange"])
                    sheet_df.to_excel(writer, sheet_name=time_range, index=False)
                    writer.sheets[time_range].write(len(sheet_df) + 1, 0, "Total Opportunities")
                    writer.sheets[time_range].write(len(sheet_df) + 1, 1, len(sheet_df))

                total_df = df.drop(columns=["TimeRange"])
                total_df.to_excel(writer, sheet_name="Total", index=False)
                writer.sheets["Total"].write(len(total_df) + 1, 0, "Total Opportunities")
                writer.sheets["Total"].write(len(total_df) + 1, 1, len(total_df))

            output.seek(0)
            
            # Construct the folder name and blob name
            folder_name = f"{current_year}/{current_month}/"
            blob_name = f"{folder_name}{sales_rep}_{current_month}_{current_year}.xlsx"
            
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.upload_blob(output, overwrite=True, content_settings=ContentSettings(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))

        return {"message": "Data processed and uploaded to Blob Storage successfully."}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"SQL query execution error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Start FastAPI with Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
