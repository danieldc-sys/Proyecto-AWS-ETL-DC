import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when

# --- Inicialización ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- 1. Extracción (Extract) ---

# Ruta para los registros corruptos
bad_records_path = "s3://raw-data-bucket-dc/logs_de_registros_malos/"

# Lee el archivo CSV desde el bucket raw
input_path = "s3://raw-data-bucket-dc/autos_vendidos_chile.csv"
dynamic_frame_input = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="csv",
    format_options={"delimiter": ",",
        "withHeader": True},
     additional_options={
        "badRecordsPath": bad_records_path}
)

# DataFrame de Spark para facilitar la transformación
dataframe = dynamic_frame_input.toDF()
print("Esquema de datos leídos:")
dataframe.printSchema()


# --- 2. Transformación (Transform) ---
# Asegurar que la columna 'valor' sea de tipo numérico (long)
dataframe_transformed = dataframe.withColumn("valor", col("valor").cast("long"))

# Se crea una nueva columna 'gama' basada en el precio del vehículo
dataframe_transformed = dataframe_transformed.withColumn(
    "gama",
    when(col("valor") < 12000000, "Gama de Entrada")
    .when((col("valor") >= 12000000) & (col("valor") < 25000000), "Gama Media")
    .otherwise("Gama Alta"),
)

print("Datos después de la transformación:")
dataframe_transformed.show(5)

# DynamicFrame para escribir con Glue
dynamic_frame_output = DynamicFrame.fromDF(
    dataframe_transformed, glueContext, "dynamic_frame_output"
)

# --- 3. Carga (Load) ---
# Resultado en formato Parquet en el bucket de datos procesados
output_path = "s3://processed-data-bucket-dc/catalogo_autos/"
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_output,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
)

# --- Finalización del Job ---
job.commit()
