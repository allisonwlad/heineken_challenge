# Challenge developer dataflow to Heineken

input_files -> sales.xlsx and metas.csv (send from Heineken)

notebooks ->

    - Raw data where ingest files into a delta tables

    - Common data where transform raw data and join information from another repository and get API details from Address (API MEU CEP)

    - Refined data where calculate metrics, aggregations, load dimensions and fact


![Data Model](sales_metas_model.svg "Data Model")
