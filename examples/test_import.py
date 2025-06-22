try:
    from leaf_airflow_operator.operators.leaf_operator import LeafBatchUploadOperator
    print("Importação bem-sucedida!")
except ImportError as e:
    print(f"Erro ao importar: {e}")