# Purpose : This DAG automates the convertion source: json/csv/parquet/athena to target: parquet format using EMR
# Related to : task_type - s3_to_s3
# Created: 2021-06-25
# Author: poojij
import boto3
from datetime import datetime
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.hooks import S3Hook, BaseHook

def s3_to_s3(dag,s3_path, cluster_task_name, config):
    dt = dag.default_args['start_date']
    for job_flow,job_flow_values in config['emr_config'].items():
        job_flow_values['Name'] = job_flow_values['Name'] + ' {}'.format(dt.strftime("%Y-%m-%d-%H-%M-%S")) 

    #create EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="{0}".format(cluster_task_name),
        job_flow_overrides = job_flow_values,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        dag=dag,
    )
    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{{{ task_instance.xcom_pull(task_ids='{}', key='return_value') }}}}".format(cluster_task_name),
        aws_conn_id="aws_default",
        dag=dag,
    )
    SPARK_STEPS =[]
    for i in config['steps'].values():
        temp_args = i['step_config']
        add_params = { "HadoopJarStep" : {"Jar": "command-runner.jar","Args": ["spark-submit","--deploy-mode","cluster","--master","yarn","--py-files","s3://airflow-ring-emr/pyspark_script/libs.zip","s3://airflow-ring-emr/pyspark_script/emr_pyspark.py",dt.strftime("%Y/%m/%d/%H/%M/%S"),s3_path,temp_args['Name']]}}
        temp_args.update(add_params)
    SPARK_STEPS.append(config['steps'])

    #add steps and monitor the added steps to EMR cluster
    for dag_tasks,step in SPARK_STEPS[0].items():
        step_adder = EmrAddStepsOperator(
            task_id="{}".format(dag_tasks),
            job_flow_id="{{{{ task_instance.xcom_pull(task_ids = '{}', key='return_value') }}}}".format(cluster_task_name),
            aws_conn_id="aws_default",
            steps=[step['step_config']],
            dag=dag,
        )
        step_checker = EmrStepSensor(
            task_id="monitor_step_{}".format(dag_tasks),
            job_flow_id="{{{{ task_instance.xcom_pull('{}', key='return_value') }}}}".format(cluster_task_name),
            step_id="{{{{ task_instance.xcom_pull(task_ids='{}', key='return_value')[0] }}}}".format(dag_tasks),
            aws_conn_id="aws_default",
            dag=dag,
        )
        create_emr_cluster >> step_adder
        step_adder >> step_checker
        step_checker >> terminate_emr_cluster
