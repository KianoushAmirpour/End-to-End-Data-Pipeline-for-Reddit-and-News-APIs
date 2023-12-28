from airflow.models import DagBag

def test_import_errors():
    dag_bag = DagBag(include_examples=False)
    assert not dag_bag.import_errors
    
def test_number_of_tasks():
    dag = DagBag.get_dag('Pipeline')
    assert len(dag.tasks) == 19
    
def test_number_of_retries():
    dag = DagBag.get_dag('Pipeline')
    assert dag.default_args["retries"] <= 2
    

    

    


    


    
    

