# desafio-dtz
##### __Teste com Cloud Dataflow, usando Apache Beam SDK com Python__

Este é um projeto no qual estamos utilizando a SDK do Apache Beam (Python) no serviço do Google Cloud Dataflow para realizar a ingestão dos arquivos no BigQuery.

Para executar o código use o seguinte comando (exemplo para o pipeline que processa o arquivo comp_boss.csv):

```sh
python3 -m \
    job_load_tb_comp_boss \
    --project desafio-dotz \
    --runner DataflowRunner \
    --temp_location \
    gs://desafio-dotz/temp \
    --region us-central1 \
    --save_main_session true \
    --requirements_file requirements.txt
```

