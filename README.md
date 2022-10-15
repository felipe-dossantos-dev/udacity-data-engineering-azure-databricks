# udacity-data-engineering-azure-databricks

# Build and Test

python -m venv .venv
source .venv\Scripts\activate
pip install -r dev-requirements.txt

- instalar java 8 ou 11, adicionar JAVA_HOME nas variaveis de ambiente
- baixar o binario do hadoop, adicionar o HADOOP_HOME nas variaveis de ambiente e %HADOOP_HOME%\bin no PATH
- usar o ***databricks-connect configure*** para configurar o cluster a ser usado
- rodar os scripts diretamente com o python do venv