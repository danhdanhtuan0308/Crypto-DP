from importlib import import_module


def test_prod_dag_structure():
    dag_module = import_module("dags.batch_etl_dag")
    dag = dag_module.dag

    assert dag.dag_id == "batch_etl_1hour_prod"
    assert dag.schedule_interval == "0 * * * *"
    assert dag.catchup is False

    task_ids = {t.task_id for t in dag.tasks}
    assert {"collect_and_aggregate", "write_to_gcs", "validate_gcs_data"}.issubset(task_ids)
