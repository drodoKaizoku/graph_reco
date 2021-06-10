#!/bin/sh

docker exec -it 12a46fc4fd32 sh -c "airflow initdb && airflow trigger_dag -c '{\"date\":20191201}' DATE_ADN"


