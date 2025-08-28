import json
import boto3

print('Cargando función...')

glue = boto3.client('glue')

def lambda_handler(event, context):
    # Nombre del trabajo de AWS Glue
    glue_job_name = "procesar_datos_autos"
    
    print(f"## EVENTO RECIBIDO: {json.dumps(event)}")
    
    try:
        # Iniciar ejecucion Glue
        response = glue.start_job_run(JobName=glue_job_name)
        
        # Imprimir la respuesta en los logs
        print(f"## TRABAJO DE GLUE INICIADO: {glue_job_name}")
        print(f"## ID DE EJECUCIÓN: {response['JobRunId']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'El trabajo de Glue {glue_job_name} se ha iniciado con éxito.')
        }
    except Exception as e:
        print(f"Error al iniciar el trabajo de Glue: {e}")
        raise e
